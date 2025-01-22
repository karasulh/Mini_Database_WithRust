use std::{fs::{read_dir, remove_file, File, OpenOptions}, io::{self, BufReader, BufWriter, Read, Write}, path::{Path, PathBuf}, time::{SystemTime, UNIX_EPOCH}};


//MemTable keeps a sorted list of last written records
//Also the latest records are written to WAL(Write Ahead Log) for recovery of MemTable.
//When MemTable reaches to Max Capacity, it is flushed to disk as a SSTable(Sorted String Table).


//TODO: Choose MemTable and SSTable entries vector max capacity as 30, for example. 
//TODO: When MemTable reaches to max capacity, it writes its entries to SSTable.
//TODO: When SSTable also reaches to max capacity, do compaction and delete the key-value WALEntry which has false tombstone. 
//TODO: DB GET,SET,DELETE commands should be applied to SSTable entries if MemTable doesnot include the mentioned key.
//TODO: Divide this lib to modules to organize well


pub struct SSTable{
    entries: Vec<SSTableEntry>,
    entry_count: u16
}

pub struct SSTableEntry{
    entries: Vec<MemTable>,
    entry_count: u16,
    table_id: u16
}

pub struct MemTable{
    entries: Vec<MemTableEntry>,
    size: usize
}


pub struct MemTableEntry{
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp:u128, //in microseconds
    pub deleted:bool //Tombstone => to show record is deleted or should be deleted with this key
}

pub struct WALEntry{
    pub key:Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool,
}


pub struct WalIterator{ //Used to iterate and read WAL file 
    reader: BufReader<File>,
}

pub struct WAL{
    path:PathBuf,
    file:BufWriter<File>
}

#[derive(Debug)]
pub struct DatabaseEntry{
    key:Vec<u8>,
    value:Vec<u8>,
    timestamp:u128
}

pub struct Database{
    dir: PathBuf,
    mem_table: MemTable,
    wal: WAL
}

impl DatabaseEntry{
    pub fn key(&self) -> &[u8]{
        &self.key
    }
    pub fn value(&self) -> &[u8]{
        &self.value
    }
    pub fn timestamp(&self) -> u128{
        self.timestamp
    }
}

impl Database{
    pub fn new(dir: &str) -> Database {
        let dir = PathBuf::from(dir);
        let (wal,mem_table) = WAL::load_from_dir(&dir).unwrap();

        Database { dir, mem_table, wal }
    }
    pub fn read(&self, key:&[u8])->Option<DatabaseEntry>{
        if let Some(mem_entry) = self.mem_table.get(key){
            return Some(DatabaseEntry{
                key: mem_entry.key.clone(),
                value: mem_entry.value.as_ref().unwrap().clone(),
                timestamp: mem_entry.timestamp
            });
        }
        None
    }
    pub fn delete(&mut self, key:&[u8]) -> Result<usize,usize>{
        let timestamp = SystemTime::now()
                                            .duration_since(UNIX_EPOCH)
                                            .unwrap()
                                            .as_micros();
        let wal_res = self.wal.delete(key, timestamp);
        if wal_res.is_err(){
            return Err(0);
        }
        if self.wal.flush().is_err(){
            return Err(0);
        }
        self.mem_table.delete(key, timestamp);

        Ok(1)
    }
    
    pub fn set(&mut self, key:&[u8], value:&[u8]) -> Result<usize,usize>{ //Create and Update operation
        let timestamp = SystemTime::now()
                                            .duration_since(UNIX_EPOCH)
                                            .unwrap()
                                            .as_micros();
        let wal_res = self.wal.set(key,value,timestamp);
        if wal_res.is_err(){
            return Err(0);
        }
        if self.wal.flush().is_err(){
            return Err(0);
        }
        self.mem_table.set(key,value,timestamp);

        Ok(1)
    }
}


impl MemTable{

    pub fn new()->MemTable {
        MemTable{
            entries:Vec::new(),
            size:0
        }
    }

    //To find a record in MemTable,helper functions. 
    //If record found and returns Ok:return index of record, if returns Error: returns index to record
    fn get_index(&self,key:&[u8]) -> Result<usize,usize>{
        self.entries.binary_search_by_key(&key, |e|e.key.as_slice())
    }

    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128){

        let entry = MemTableEntry{
            key: key.to_owned(),
            value: Some(value.to_owned()),
            timestamp,
            deleted:false
        };

        match self.get_index(key){
            Ok(idx) => {
                //if Key-Value exist, add difference btw new and old value to MemTable's size
                if let Some(v) = self.entries[idx].value.as_ref(){
                    if value.len() < v.len(){
                        self.size -= v.len()-value.len();
                    }
                    else{
                        self.size += value.len() - v.len();
                    }
                }
                self.entries[idx] = entry;
            }
            Err(idx) => {
                //if Key-Value does not exist:
                self.size += key.len() + value.len() + 16 + 1; //increase the size of Memtable with Key+Value+TimeStamp+Tombstone(boolean) sizes
                self.entries.insert(idx, entry);
            }
        }

    }

    pub fn delete(&mut self, key: &[u8], timestamp: u128){

        let entry = MemTableEntry{
            key: key.to_owned(),
            value: None,
            timestamp,
            deleted: true
        };

        match self.get_index(key){
            Ok(idx) => {
                //if Value exist, delete its value from MemTable's size
                if let Some(value) = self.entries[idx].value.as_ref(){
                        self.size -= value.len();
                }
                self.entries[idx] = entry;
            }
            Err(idx) => {
                //even if there is no record, add its entry to MemTable, because maybe it is in SSTable.
                //When Compaction step, according to this "deleted"(Tombstone) boolean, it will delete records in SSTable. 
                self.size += key.len() + 16 + 1; //increase the size of Memtable with Key+Value+TimeStamp+Tombstone(boolean) sizes
                self.entries.insert(idx, entry);
            }
        }
    }

    pub fn get(&self, key:&[u8])->Option<&MemTableEntry>{
        if let Ok(idx) = self.get_index(key){
            return Some(&self.entries[idx]);
        }
        None
    }

}


impl WalIterator{
    pub fn new(path:PathBuf)->io::Result<WalIterator>{

        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(WalIterator{reader})
    }
}

impl Iterator for WalIterator{
    type Item = WALEntry;

    fn next(&mut self) -> Option<Self::Item> {
        
        let mut len_buffer = [0;8];
        if self.reader.read_exact(&mut len_buffer).is_err() {
            return None;
        }
        let key_len = usize::from_le_bytes(len_buffer);
        
        let mut bool_buffer = [0; 1];
        if self.reader.read_exact(&mut bool_buffer).is_err() {
            return None;
        }
        let deleted = bool_buffer[0] != 0;

        let mut key = vec![0; key_len];
        let mut value = None;
        if deleted {
            if self.reader.read_exact(&mut key).is_err() {
                return None;
            }
        } else {
            if self.reader.read_exact(&mut len_buffer).is_err() {
                return None;
            }
            let value_len = usize::from_le_bytes(len_buffer);
            if self.reader.read_exact(&mut key).is_err() {
                return None;
            }
            let mut value_buf = vec![0; value_len];
            if self.reader.read_exact(&mut value_buf).is_err() {
                return None;
            } 
            value = Some(value_buf);
        }

        let mut timestamp_buffer = [0; 16];
        if self.reader.read_exact(&mut timestamp_buffer).is_err() {
            return None;
        }
        let timestamp = u128::from_le_bytes(timestamp_buffer);

        Some(WALEntry {
        key,
        value,
        timestamp,
        deleted,
        })
  }
}



impl WAL{
    //Create a new WAL in this directory path
    pub fn new(dir:&Path)->io::Result<WAL>{
        let timestamp = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap()
                                        .as_micros();
        let path = Path::new(dir).join(timestamp.to_string() + ".wal");
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);

        Ok( WAL{path,file} )
    }

    // Load already exist WAL from path
    pub fn from_path(path: &Path) -> io::Result<WAL> {
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        let file = BufWriter::new(file);
  
        Ok(WAL {path: path.to_owned(),file})
    }

    // | Key Size (8B) | Tombstone(1B) | Value Size (8B) | Key | Value | Timestamp (16B) |
    pub fn set(&mut self, key:&[u8], value: &[u8], timestamp: u128) -> io::Result<()>{
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(false as u8).to_le_bytes())?;
        self.file.write_all(&value.len().to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(value)?;
        self.file.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }

    pub fn delete(&mut self, key:&[u8], timestamp: u128) -> io::Result<()>{
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(true as u8).to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }

    pub fn flush(&mut self)-> io::Result<()>{ //Flushes the WAL to disk
        self.file.flush()
    }

    pub fn load_from_dir(dir: &Path) -> io::Result<(WAL,MemTable)>{
        let mut wal_files = Vec::<PathBuf>::new();
        for file in read_dir(dir).unwrap(){
            let path = file.unwrap().path();
            if path.extension().unwrap() == "wal" {
                wal_files.push(path);
            }
        }
        wal_files.sort();

        let mut new_mem_table = MemTable::new();
        let mut new_wal = WAL::new(dir)?;
        for wal_file in wal_files.iter(){
            if let Ok(wal) = WAL::from_path(wal_file){
                for entry in wal.into_iter(){
                    if entry.deleted {
                        new_mem_table.delete(entry.key.as_slice(), entry.timestamp);
                        new_wal.delete(entry.key.as_slice(), entry.timestamp)?;
                    }
                    else {
                        new_mem_table.set(entry.key.as_slice(), entry.value.as_ref().unwrap().as_slice(), entry.timestamp);
                        new_wal.set(entry.key.as_slice(), entry.value.unwrap().as_slice(), entry.timestamp)?;
                    }
                }
            }
        }
        new_wal.flush().unwrap();
        wal_files.into_iter().for_each(|f| remove_file(f).unwrap());
        
        Ok((new_wal,new_mem_table))
    }
}

impl IntoIterator for WAL{
    type Item = WALEntry;

    type IntoIter = WalIterator;

    fn into_iter(self) -> Self::IntoIter {
        WalIterator::new(self.path).unwrap()
    }
}





