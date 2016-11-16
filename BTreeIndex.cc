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
using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid =0;
    treeHeight=0;
    keycount=0;
    maxkey=0;
    minkey=0;
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
    RC rc;
    char buffer[PageFile::PAGE_SIZE];
    char * tmp=buffer;
    rc=pf.open(indexname+".idx", mode);
    rc=pf.read(0, buffer);
    if ((pf.endPid()==0)&&((mode=='w')||(mode=='W'))) {
        rootPid=0;
        treeHeight=0;
        keycount=0;
        maxkey=0;
        minkey=0;
        openmode=mode;
        return 0;
        
    }
    else
    {
        memcpy(&rootPid,tmp,sizeof(int));
        tmp=tmp+sizeof(int);
        memcpy(&treeHeight,tmp,sizeof(int));
        tmp=tmp+sizeof(int);
        memcpy(&keycount,tmp,sizeof(int));
        tmp=tmp+sizeof(int);
        memcpy(&maxkey,tmp,sizeof(int));
        tmp=tmp+sizeof(int);
        memcpy(&minkey,tmp,sizeof(int));
        visited=new PageId[treeHeight];
        openmode=mode;
    }
    
    return rc;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
    RC rc;
    if ((openmode=='w')||(openmode=='W')) {
        char buffer[PageFile::PAGE_SIZE];
        char * tmp=buffer;
        memcpy(tmp,&rootPid,sizeof(int));
        tmp=tmp+sizeof(int);
        memcpy(tmp,&treeHeight,sizeof(int));
        tmp=tmp+sizeof(int);
        memcpy(tmp,&keycount,sizeof(int));
        tmp=tmp+sizeof(int);
        memcpy(tmp,&maxkey,sizeof(int));
        tmp=tmp+sizeof(int);
        memcpy(tmp,&minkey,sizeof(int));
        rc=pf.write(0, buffer);
    }
    delete[] visited;
    rc=pf.close();
    return rc;
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
    RC rc;
    BTNonLeafNode nonleaf;
    IndexCursor cursor;
    BTLeafNode leaf;
    BTLeafNode leafsibling;
    BTNonLeafNode nonleafsibling;
    PageId pid;
    int midkey;
    int siblingkey;
   
    if (treeHeight==0) {
        leaf=BTLeafNode();
        rc=leaf.insert(key, rid);
        rootPid=pf.endPid()+1;
        rc=leaf.write(rootPid, pf);
        treeHeight=1;
        visited=new PageId[treeHeight];
        maxkey=key;
        minkey=key;
        keycount++;
        return rc;

    }
    else
    {
        rc=locate(key, cursor);
        if (rc==0) {
            rc=RC_INVALID_ATTRIBUTE;
            return rc;
        }
        else
        {
            if (key>maxkey) {
                maxkey=key;
            }
            if (key<minkey) {
                minkey=key;
            }
            int visiting=treeHeight-1;
            leaf=BTLeafNode();
            rc=leaf.read(visited[visiting], pf);
            rc=leaf.insert(key, rid);
            if (rc==0) {
                leaf.write(visited[visiting], pf);
                return rc;
            }
            else
            {
                keycount++;
                leafsibling=BTLeafNode();
                rc=leaf.insertAndSplit(key, rid, leafsibling, siblingkey);
                pid=pf.endPid();
                PageId tmppid;
                tmppid=leaf.getNextNodePtr();
                leafsibling.setNextNodePtr(tmppid);
                leaf.setNextNodePtr(pid);
                rc=leafsibling.write(pid, pf);
                rc=leaf.write(visited[visiting], pf);
                if (treeHeight>1)
                {
                    visiting--;
                    nonleaf=BTNonLeafNode();
                    rc=nonleaf.read(visited[visiting], pf);
                    rc=nonleaf.insert(siblingkey, pid);
                    if (rc==0) {
                        rc=nonleaf.write(visited[visiting], pf);
                        return rc;
                    }
                    else
                    {
                        for (int i=1; i<treeHeight-1; i--)
                        {
                            nonleafsibling=BTNonLeafNode();
                            rc=nonleaf.insertAndSplit(siblingkey, pid, nonleafsibling, midkey);
                            siblingkey=midkey;
                            pid=pf.endPid();
                            rc=nonleafsibling.write(pid, pf);
                            rc=nonleaf.write(visited[visiting], pf);
                            visiting--;
                            nonleaf=BTNonLeafNode();
                            rc=nonleaf.read(visited[visiting], pf);
                            rc=nonleaf.insert(siblingkey, pid);
                            if (rc==0) {
                                rc=nonleaf.write(visited[visiting], pf);
                                return rc;
                            }
                        }
                        nonleafsibling=BTNonLeafNode();
                        rc=nonleaf.insertAndSplit(siblingkey, pid, nonleafsibling, midkey);
                        siblingkey=midkey;
                        pid=pf.endPid();
                        rc=nonleafsibling.write(pid, pf);
                        rc=nonleaf.write(visited[visiting], pf);
                        BTNonLeafNode newroot;
                        newroot=BTNonLeafNode();
                        rc=newroot.initializeRoot(visited[visiting], siblingkey, pid);
                        rootPid=pf.endPid();

                        rc=newroot.write(rootPid, pf);
                        treeHeight++;
                        delete [] visited;
                        visited=new PageId[treeHeight];
                        return rc;
                    }
                }
                    else
                    {
                        BTNonLeafNode newroot;
                        newroot=BTNonLeafNode();
                        rc=newroot.initializeRoot(visited[visiting], siblingkey, pid);
                        rootPid=pf.endPid();
                        rc=newroot.write(rootPid, pf);
                        treeHeight++;
                        delete [] visited;
                        visited=new PageId[treeHeight];
                        return rc;

                    }

            }
        }
    }
    return rc;
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
        RC rc;
        int visiting=0;
        BTNonLeafNode nonleaf;
        BTLeafNode leaf;
        visited[visiting]=rootPid;
        PageId pid=rootPid;
        int eid=-1;
//when treehight>1 means the root is a nonleaf
        if (treeHeight>1) {
            rc=nonleaf.read(pid, pf);
                for (int i=1; i<treeHeight-1; i++) {
                rc=nonleaf.locateChildPtr(searchKey, pid);
                rc=nonleaf.read(pid, pf);
                visiting++;
                visited[visiting]=pid;

            }
            rc=nonleaf.locateChildPtr(searchKey, pid);
            leaf.read(pid, pf);
            visiting++;
            visited[visiting]=pid;
            rc=leaf.locate(searchKey, eid);
            cursor.pid=pid;
            cursor.eid=eid;
            return rc;

        }
        else
    //when treeheight=1, which means the root is a leafnode;
        {
            rc=leaf.read(rootPid, pf);
            rc=leaf.locate(searchKey, eid);
            cursor.pid=rootPid;
            cursor.eid=eid;
            return rc;
            
        }
        return rc;
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
    RC rc;
    BTLeafNode leaf;
    leaf.read(cursor.pid, pf);
    rc=leaf.readEntry(cursor.eid, key, rid);
    if (cursor.eid<leaf.getKeyCount()-1) {
        cursor.eid++;
    }
    else
    {
        cursor.pid=leaf.getNextNodePtr();
        cursor.eid=0;
    }
    return rc;
}
int BTreeIndex::getKeycount()
{
    return keycount;
}
int BTreeIndex::getmaxKey()
{
    return maxkey;
}
int BTreeIndex::getminKey()
{
    return minkey;
}
