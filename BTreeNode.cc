#include "BTreeNode.h"

using namespace std;

BTLeafNode::BTLeafNode() {
    LeafNodeHeader* header = (LeafNodeHeader*) buffer;
    header->num_keys = 0;
}

BTLeafNode::BTLeafNode(PageId prev, PageId next) {
    LeafNodeHeader* header = (LeafNodeHeader*) buffer;
    header->previous_page = prev;
    header->next_page = next;
    header->num_keys = 0;
}

/*
 * Return the pid of the next sibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getPrevNodePtr() {
    LeafNodeHeader* header = (LeafNodeHeader*) buffer;
    return header->previous_page;
}

/*
 * Set the pid of the next sibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setPrevNodePtr(PageId pid) {
    LeafNodeHeader* header = (LeafNodeHeader*) buffer;
    header->previous_page = pid;
    return 0;
}

PageId BTLeafNode::getPid() {
    LeafNodeHeader* header = (LeafNodeHeader*) buffer;
    return header->pid;
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{ 
    return pf.read(pid, buffer); 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ 
    return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{
    LeafNodeHeader * header = (LeafNodeHeader*) buffer; 
    return header->num_keys;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{
    LeafNodeHeader * header = (LeafNodeHeader*) buffer; 
    int n_keys = header->num_keys;
    if ( n_keys >= MAX_LEAF_PAIRS )
        return RC_NODE_FULL;

    LeafPair tmp_pair; 

    LeafPair storage_pair;
    storage_pair.rid = rid;
    storage_pair.key = key;

    LeafPair* pair; 
    int i = sizeof(LeafNodeHeader); 
    for (;  i < sizeof(LeafNodeHeader) + n_keys * sizeof(LeafPair); 
            i += sizeof(LeafPair))
    {
        //WHY ARE WE STARTING AT INDEX 1 AS OPPOSED TO INDEX 0
        //COULD BE NULL
        //doesn't this do a swap, not an insertion?
        pair = (LeafPair*) (buffer + i);
        // should move the entirety of the array, I think. 
        if ( pair->key > storage_pair.key )
        {
            tmp_pair.key = pair->key;
            tmp_pair.rid = pair->rid;

            pair->key = storage_pair.key;
            pair->rid = storage_pair.rid;

            // can we just do "storage_pair = tmp_pair;"? 
            storage_pair.key = tmp_pair.key;
            storage_pair.rid = tmp_pair.rid;
        }
    }
    pair =(LeafPair*)  (buffer + i);
    pair->key = storage_pair.key;
    pair->rid = storage_pair.rid;

    header->num_keys++;

    return 0; 
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{
    // force sibling to be empty
//    BTLeafNode node;
//    sibling = node;
    
    int n_keys = getKeyCount();
    if (n_keys != MAX_LEAF_PAIRS) {
        return 1;   //todo: get correct error code
    }
    int loc;
    RC val = locate(key, loc);
    if (val == 0) {
        return 1; // todo: we found it for some weird reason get correct error code
    }
    
    if (val <= n_keys/2) {
        LeafPair* orig_pair = (LeafPair*) (buffer + sizeof(LeafNodeHeader) + (n_keys/2) * sizeof(LeafPair));
        sibling.insert(orig_pair->key, orig_pair->rid);
    }
    for (int i = n_keys/2 + 1; i < n_keys; i++) {
        LeafPair* orig_pair = (LeafPair*) (buffer + sizeof(LeafNodeHeader) + i * sizeof(LeafPair));
        sibling.insert(orig_pair->key, orig_pair->rid);
    }
    
    //update headers
    LeafNodeHeader header = (LeafNodeHeader*) buffer;
    sibling.setPrevNodePtr(header.pid);
    sibling.setNextNodePtr(header.next_page);    
    
    header.num_keys = n_keys/2 + (val <= n_keys/2);
    header.next_page = sibling.getPid();
    
    //insert new value
    (val <= n_keys/2) ? insert(key, rid) : sibling.insert(key, rid);    
    return 0;
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{

    // currently not binary search. TODO: change for improved efficiency? 
    // can we do and figure out the entry id? probably huh. but it's annoying. 
    int entry_id = 0; 
    for ( int i = sizeof(LeafNodeHeader); 
            i < sizeof(LeafNodeHeader) + getKeyCount() * sizeof(LeafPair); 
            i += sizeof(LeafPair))
    {
        LeafPair* pair = (LeafPair*) (buffer + i);
        if ( pair->key == searchKey )
        {
            eid = entry_id;
            return 0;
        }
        else if ( pair->key < searchKey )
        {
            eid = entry_id; 
            return RC_NO_SUCH_RECORD;
        }
        entry_id++;
    }
    eid = entry_id; 
    return RC_NO_SUCH_RECORD;
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{ return 0; }

/*
 * Return the pid of the next sibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{
    LeafNodeHeader* header = (LeafNodeHeader*) buffer;
    return header->next_page;
}

/*
 * Set the pid of the next sibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{
    LeafNodeHeader* header = (LeafNodeHeader*) buffer;
    header->next_page = pid;
    return 0;
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ return 0; }
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ return 0; }

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{ return 0; }


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ return 0; }

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ return 0; }

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ return 0; }

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ return 0; }
