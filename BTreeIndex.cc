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
#include <iostream>
#include <fstream>

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
    RC rc = pf.open(indexname, mode);
    if (rc)
        return rc;
    
    // we are initializing a BTreeIndex. 
    if (pf.endPid() == 0)
    {
        // initialize a new index. 
        treeHeight = 0;
    } 
    else
    {
        // here, we assume that the page file contains an index. 
        char buffer[PageFile::PAGE_SIZE]; // this fucking line. 
        // we put the Header in this file.
        rc = pf.read(0, buffer);  
        if (rc) 
        {
            return rc;
        }

        Header* header = (Header *)buffer; 
        if ( !(header->initialized) )
        {
            fprintf(stdout, "this thing isn't initialized?!\n");
            return 1; // something is wrong with the buffer setup.
        } 
        //fprintf(stdout, "bruh\n");
        treeHeight = header->treeHeight;
        //fprintf(stdout, "treeheight: %d\n", treeHeight);
        rootPid = header->rootPid;
        //fprintf(stdout, "rootPid: %d\n", rootPid);
    }
    fprintf(stdout, "fml\n");
    return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
    // this code puts all the information back into the page file. 
    // the problem with this code may be that there are atomicity issues in the future. 
    // we are not sure how the read write privileges provided by page file works 
    // so we don't want to guarantee that our code will remain this way. 
    RC rc;
    char buffer[PageFile::PAGE_SIZE]; 
    rc = pf.read(0, buffer);
    if (rc)
        return rc;   
    
    Header* header = (Header *)buffer; 
    header->initialized = true;
    header->treeHeight = treeHeight;
    header->rootPid = rootPid;
    fprintf(stdout, "treeheight: %d\n", treeHeight);
    fprintf(stdout, "rootPid: %d\n", rootPid);
    fprintf(stdout, "IN CLOSE: pf.endPid(): %d\n", pf.endPid());
    rc = pf.write(0, buffer);
    if (rc)
        return rc;
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
    if (treeHeight == 0) {
        //initialize a new tree
        // we assume a new tree will have its root be a leaf. 
        BTLeafNode root;
        rootPid = 1;
        root.write(rootPid, pf);
        treeHeight = 1;
        root.insert(key, rid);
    } else {
        IndexCursor cursor;
        vector<PageId> path;
        // sets path and cursor. 
        locate(key, cursor, rootPid, 1, path);
        PageId leafId = path.back();
        path.pop_back();
        BTLeafNode leaf;
        leaf.read(leafId, pf);


        if (leaf.insert(key, rid)) { 
            BTLeafNode sibling;
            int siblingKey;
            leaf.insertAndSplit(key, rid, sibling, siblingKey);

            // save the new leaves. 
            leaf.write(leaf.getPid(), pf);
            sibling.write(pf.endPid(), pf);

            BTNonLeafNode parent;

            if (path.empty())
            {
                // creates the first nonleaf root. 
                PageId new_root_id = pf.endPid();
                BTNonLeafNode new_root;
                treeHeight++;
                rootPid = new_root_id;
                new_root.read(new_root_id, pf);
                new_root.initializeRoot(leafId, siblingKey, sibling.getPid());
                new_root.write(new_root_id, pf);
            }
            else
            {
                // propogate up the new key to add into stuff. 
                PageId parentId = path.back(); 
                path.pop_back();
                parent.read(parentId, pf);
            
                // we need to insert the sibling's key into the parent. 
                while (parent.insert(siblingKey, sibling.getPid())) {
                    BTNonLeafNode siblingNonLeaf;
                    int midKey;
                    parent.insertAndSplit(siblingKey, sibling.getPid(), siblingNonLeaf, midKey);

                    parent.write(parentId, pf);
                    PageId siblingId = pf.endPid();
                    siblingNonLeaf.write(siblingId, pf);

                    if (path.empty()) {
                        //leaf is old root node, 
                        //need to increase tree height,
                        //create new root node
                        //update first page in pagefile to say where new root node is
                        BTNonLeafNode newRoot;
                        PageId newRootId = pf.endPid();
                        treeHeight++;
                        rootPid = newRootId;
                        newRoot.write(newRootId, pf);
                        newRoot.initializeRoot(parentId, midKey, siblingId);
                        break;
                    } else {
                        // we still have parents to go up to. 
                        // and we still have to split. 
                        siblingKey = midKey;
                        parentId = path.back();
                        path.pop_back();

                        parent.read(parentId, pf);
                    }
                }
            parent.write(parentId, pf);
            }
        } else {
            leaf.write(leaf.getPid(), pf);
        }
    }
    return 0;
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

/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. This is simply a recursive
 * helper function that allows us to have slightly more control
 * over the parameters that we want to have input and output for 
 * easier coding purposes. 
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @param cur_page[IN] the current page id of the node we are looking at. 
 * @param level[IN] the current level of the B+-tree we are looking at. 
 * @param path[OUT] the "lineage" of the search we have gone through in the past. 
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor, 
                      PageId cur_page, int level, vector<PageId>& path)
{
    fprintf(stdout, "searchkey: %d, cur_page: %d\n", searchKey, cur_page);
    fprintf(stdout, "cur_level: %d, height: %d\n", level, treeHeight);
    path.push_back(cur_page);
    if ( level == treeHeight )
    {
        BTLeafNode leaf;
        fprintf(stdout, "kill me baby.\n");
        leaf.read(cur_page, pf);
        
        int eid;
        int key; 
        RecordId rid; 
        fprintf(stdout, "jingle bells.\n");
        RC val = leaf.locate(searchKey, eid);
        fprintf(stdout, "oh what fun.\n");
        leaf.readEntry(eid, key, rid);
        fprintf(stdout, "it is to ride.\n");

        cursor.pid = cur_page;
        cursor.eid = eid;
        return val;
    }

    BTNonLeafNode node;
    node.read(cur_page, pf);

    PageId next_page;
    node.locateChildPtr(searchKey, next_page); // currently always returns 0. 
    fprintf(stdout, "next_page: %d\n", next_page);
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
    leaf.read(cursor.pid, pf);
    RC successfulRead = leaf.readEntry(cursor.eid, key, rid);
    if ( cursor.eid == leaf.getKeyCount() ) {
        cursor.pid = leaf.getNextNodePtr();
        cursor.eid = 0;
    }
    else
        cursor.eid = cursor.eid + 1;
    return successfulRead;
}
