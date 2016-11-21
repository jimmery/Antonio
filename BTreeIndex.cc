/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = -1;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
    return pf.open(indexname, mode);
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
    return pf.close();
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
    IndexCursor cursor;
    vector<PageId> path;
    locate(key, cursor, rootPid, 1, path);
    PageId leafId = path.back();
    path.pop_back();
    BTLeafNode leaf;
    leaf.read(leafId, pf);
    BTLeafNode sibling;
    int siblingKey;
    
    if (leaf.insert(key, rid)) {
        leaf.insertAndSplit(key, rid, sibling, siblingKey);
        leaf.write(leaf.getPid(), pf);
        
        //right now sibling pid isn't set.
        //TODO: FIX THIS
        sibling.write(sibling.getPid(), pf);
        BTNonLeafNode parent;
        PageId parentId = path.back();
        path.pop_back();
        parent.read(parentId, pf);
        while (parent.insert(siblingKey, sibling.getPid())) {
            BTNonLeafNode siblingNonLeaf;
            int midKey;
            parent.insertAndSplit(siblingKey, sibling.getPid(), siblingNonLeaf, midKey);
            
            parent.write(parentId, pf);
            
            siblingKey = midKey;
            parentId = path.back();
            path.pop_back();
            
            parent.read(parentId, pf);
        }
        parent.write(parentId, pf);
    } else {
        leaf.write(leaf.getPid(), pf);
    }
    
    // disregard
    for (int i = 0; i < path.size()-1; i++) {
        BTNonLeafNode node;
        if (node.insert(key, rid)) {
            node.insertAndSplit(key, rid);
            BTLeafNode leaf;
            if (leaf.insert(key, rid))
                leaf.insertAndSplit(key, rid);
        }
    }
}

/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
    vector<PageId> path;
    return locate(searchKey, cursor, rootPid, 1, path);
}

RC BTreeIndex::locate(int searchKey, IndexCursor& cursor, 
                      PageId cur_page, int level, vector<PageId>& path)
{
    path.push_back(cur_page);
    if ( level == treeHeight )
    {
        BTLeafNode leaf;
        leaf.read(cur_page, pf);

        int eid;
        int key; 
        RecordId rid; 
        RC val = leaf.locate(searchKey, eid);
        leaf.readEntry(eid, key, rid);

        cursor.pid = cur_page;
        return val;
    }

    BTNonLeafNode node;
    node.read(cur_page, pf);

    PageId next_page;
    node.locateChildPtr(searchKey, next_page); // currently always returns 0. 
    return locate(searchKey, cursor, next_page, level + 1, path);
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
    BTLeafNode leaf;
    int successfulRead = leaf.readEntry(cursor.eid, key, rid);
    leaf.locate(key+1, cursor.eid);
    return successfulRead;
}
