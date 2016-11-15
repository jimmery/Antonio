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
    RC val = pf.read(pid, buffer); 
    LeafNodeHeader* header = (LeafNodeHeader*) buffer; 
    if (header->pid != pid) {
        // maybe make an indication saying that this is a completely 
        // new or empty node. 
        header->pid = pid;
    }
    return val;
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ 
    LeafNodeHeader* header = (LeafNodeHeader*) buffer; 
    if (header->pid != pid) {
        // maybe make an indication that the pid changed. 
        header->pid = pid;
    }
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

    for (int i = 0; i < n_keys; i++)
    {
        pair = (LeafPair*) (buffer + byteIndexOf(i));
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
    pair = (LeafPair*)(buffer + byteIndexOf(n_keys));
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
    
    if (loc <= n_keys/2) {
        LeafPair* orig_pair = (LeafPair*) (buffer + byteIndexOf(n_keys/2));
        sibling.insert(orig_pair->key, orig_pair->rid);
    }
    for (int i = n_keys/2 + 1; i < n_keys; i++) {
        LeafPair* orig_pair = (LeafPair*) (buffer + byteIndexOf(i));
        sibling.insert(orig_pair->key, orig_pair->rid);
    }
    
    //update headers
    LeafNodeHeader* header = (LeafNodeHeader*) buffer;
    sibling.setPrevNodePtr(header->pid);
    sibling.setNextNodePtr(header->next_page);    
    
    header->num_keys = n_keys/2 + (loc <= n_keys/2);
    header->next_page = sibling.getPid();
    
    //insert new value
    (loc <= n_keys/2) ? insert(key, rid) : sibling.insert(key, rid);    

    RecordId r;
    val = sibling.readEntry(0, siblingKey, r);
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
    for (int i = 0; i < getKeyCount(); i++)
    {
        LeafPair* pair = (LeafPair*) (buffer + byteIndexOf(i));
        if (pair->key == searchKey)
        {
            eid = i;
            return 0;
        }
        else if ( pair->key < searchKey )
        {
            eid = i; 
            return RC_NO_SUCH_RECORD;
        }
    }
    eid = getKeyCount(); 
    return RC_NO_SUCH_RECORD;

    // // implemented binary search. i don't actually think we need this. 
    // // TODO: check if this works. 
    // int n_keys = getKeyCount();
    // for (int i = n_keys / 2; ; )
    // {
    //     LeafPair* pair = (LeafPair*) (buffer + byteIndexOf(i));
    //     if (searchKey == pair->key)
    //     {
    //         eid = i;
    //         return 0;
    //     }
    //     else if (searchKey < pair->key)
    //     {
    //         // slightly more complexity in figuring out if this current node
    //         // is the smallest node greater than it. 
    //         LeafPair* prev_pair = (LeafPair*) (buffer + byteIndexOf(i-1));
    //         if (searchKey == prev_pair->key ) // this case can probably be removed if needed. 
    //         {
    //             eid = i-1;
    //             return 0;
    //         }
    //         else if (searchKey > prev_pair->key)
    //         {
    //             eid = 1; 
    //             return RC_NO_SUCH_RECORD;
    //         }
    //         i = i / 2;
    //     }
    //     else
    //     {
    //         i = (i + n_keys) / 2;
    //     }
    // }
    // eid = getKeyCount(); 
    // return RC_NO_SUCH_RECORD;
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{
    if ( eid < 0 || eid >= getKeyCount() )
    {
        return RC_INVALID_RID; // TODO check.
    }
    LeafPair* pair = (LeafPair*) (buffer + byteIndexOf(eid));
    key = pair->key;
    rid = pair->rid;
    return 0;
}

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

int BTLeafNode::byteIndexOf(int i)
{
    return sizeof(LeafNodeHeader) + i * sizeof(LeafPair);
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{
    return pf.read(pid, buffer);
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{
    return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{
    NonLeafHeader* header = (NonLeafHeader*) buffer;
    return header->num_keys;
}


/*
 * Insert a (key, pid) pair to the node.
 * Here, we assume that the pid inputted will contain the pid to the leaf
 * AFTER the key. 
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{
    NonLeafHeader * header = (NonLeafHeader*) buffer; 
    int n_keys = header->num_keys;
    if ( n_keys >= MAX_NONLEAF_PAIRS )
        return RC_NODE_FULL;

    NodePair tmp_pair; 

    NodePair storage_pair;
    storage_pair.pid = pid;
    storage_pair.key = key;

    // TODO: assuming the case that insert is only ever called when there's a lower overflow
    // will there ever be a case where we insert something that requires changing the 
    // first_pid? i don't think so for leaf. 
    // what about non-leaf overflow? potentially actually. 
    // how to deal with this case? i'll try a hacky solution for now so that it doesn't. 
    NodePair* pair; 
    for (int i = 0; i < n_keys; i++)
    {
        pair = (NodePair*) (buffer + byteIndexOf(i));
        if ( pair->key > storage_pair.key )
        {
            tmp_pair.key = pair->key;
            tmp_pair.pid = pair->pid;

            pair->key = storage_pair.key;
            pair->pid = storage_pair.pid;

            // can we just do "storage_pair = tmp_pair;"? 
            storage_pair.key = tmp_pair.key;
            storage_pair.pid = tmp_pair.pid;
        }
    }

    /* TODO: CHECK HOW THE POINTERS WORK. 
        BECAUSE OF LAST PID. Like do pointers have to shift? 
        I feel like it does. */

    pair = (NodePair*)(buffer + byteIndexOf(n_keys));
    pair->key = storage_pair.key;
    pair->pid = storage_pair.pid;

    header->num_keys++;

    return 0; 
}

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
{
    // force sibling to be empty
    // BTLeafNode node;  
    // sibling = node;
    
    int n_keys = getKeyCount();
    if (n_keys != MAX_NONLEAF_PAIRS) {
        return 1;   
    }

    int loc;
    //RC val = locate(key, loc); // TODO: do we need to implement something similar? 

    for (int i = 0; i < getKeyCount(); i++)
    {
        NodePair* pair = (NodePair*) (buffer + byteIndexOf(i));
        if (key >= pair->key) // equal case shouldn't ever trigger. 
        {
            loc = i;
            break;
        }
    }
    

    if (loc == (n_keys + 1) / 2)
    {
        midKey = key;
        NodePair* pair = (NodePair*) (buffer + byteIndexOf((n_keys + 1)/2));
        sibling.initializeRoot(pid, pair->key, pair->pid);
        
        NodePair* next_pair = (NodePair*) (buffer + byteIndexOf((n_keys+1)/2 + 1));
        sibling.insert(next_pair->key, next_pair->pid);
    }
    else if (loc < (n_keys + 1) / 2)
    {
        NodePair* mid_pair = (NodePair*) (buffer + byteIndexOf((n_keys + 1)/2 - 1));
        midKey = mid_pair->key;

        NodePair* orig_pair = (NodePair*) (buffer + byteIndexOf((n_keys + 1)/2));
        sibling.initializeRoot(mid_pair->pid, orig_pair->key, orig_pair->pid);

        NodePair* next_pair = (NodePair*) (buffer + byteIndexOf((n_keys+1)/2 + 1));
        sibling.insert(next_pair->key, next_pair->pid);
    }
    else
    {
        NodePair* mid_pair = (NodePair*) (buffer + byteIndexOf((n_keys + 1)/2));
        midKey = mid_pair->key;

        NodePair* moved_pair = (NodePair*) (buffer + byteIndexOf((n_keys+1)/2 + 1));
        sibling.initializeRoot(mid_pair->pid, moved_pair->key, moved_pair->pid);
        sibling.insert(key, pid); // insert the new pair into sibling. 
    }
    
    // TODO: here, we are using insert as if we are inserting the pid before the key. 
    //       however, I am not sure if this is the correct usage right now. 
    // TODO: check how insertion works in this case. 
    //       I have a feeling we remove the key returned up. 
    for (int i = (n_keys + 1)/2 + 2; i < n_keys; i++) 
    {
        NodePair* orig_pair = (NodePair*) (buffer + byteIndexOf(i));
        sibling.insert(orig_pair->key, orig_pair->pid);
    }

    // TODO: there's something about this that I can't figure out?
    LeafNodeHeader* header = (LeafNodeHeader*) buffer;
    header->num_keys = (n_keys+1)/2; // + (loc < (n_keys+1)/2); ? 
    if (loc < (n_keys + 1) / 2)
        insert(key, pid); // insert the new pair into us. 
    return 0;
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{
    NodePair* pair = (NodePair*) (buffer + byteIndexOf(0));
    if (searchKey > pair->key)
    {
        NonLeafHeader* header = (NonLeafHeader*) buffer; 
        pid = header->first_pid;
        return 0;
    }
    for (int i = 1; i < getKeyCount(); i++)
    {
        pair = (NodePair*) (buffer + byteIndexOf(i));
        if ( searchKey > pair->key )
        {
            NodePair* prev_pair = (NodePair*) (buffer + byteIndexOf(i-1));
            pid = prev_pair->pid;
            return 0;
        }
    }
    return 0;
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{
    NonLeafHeader* header = (NonLeafHeader*) buffer; 
    header->num_keys = 1;
    header->first_pid = pid1;

    NodePair* pair = (NodePair*) (buffer + sizeof(NonLeafHeader));
    pair->key = key;
    pair->pid = pid2;
    return 0;
}

int BTNonLeafNode::byteIndexOf(int i)
{
    return sizeof(NonLeafHeader) + i * sizeof(NodePair);
}
