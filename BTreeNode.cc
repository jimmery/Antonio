#include "BTreeNode.h"

using namespace std;

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
    return header->num_nodes;
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
    int n_keys = header->num_nodes;
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
    pair =(LeafPair*)  buffer + i;
    pair.key = storage_pair.key;
    pair.rid = storage_pair.rid;

    header->num_nodes++;

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
    LeafNodeFooter* foot = (LeafNodeFooter*) (buffer + FOOTER_POSITION);
    return foot->next_page;
}

/*
 * Set the pid of the next sibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{
    LeafNodeFooter* foot = (LeafNodeFooter*) (buffer + FOOTER_POSITION);
    return foot->next_page = pid;
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
