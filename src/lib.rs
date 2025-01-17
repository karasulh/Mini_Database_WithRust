
//MemTable keeps a sorted list of last written records
//Also the latest records are written to WAL(Write Ahead Log) for recovery of MemTable.
//When MemTable reaches to Max Capacity, it is flushed to disk as a SSTable(Sorted String Table).
//TODO: Entries Vector vs HashMap ??
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
                //if Value exist, add difference btw new and old value to MemTable's size
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


