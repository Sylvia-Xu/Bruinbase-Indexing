#include "BTreeNode.h"
#include <string.h>
#include <iostream>
using namespace std;

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::print()
{
    cout<<"-------------"<<endl;
    cout<<"This is a leaf"<<endl;
    cout<<"key_rid"<<endl;
    for (int i=0; i<keycount; i++) {
        cout<<"item"<<i<<'\t'<<"key="<<key_rid[i].key<<'\t'<<"rid="<<key_rid[i].rid.pid<<'\t'<<key_rid[i].rid.sid<<endl;
    }
    cout<<"keycount:"<<keycount<<endl;
    cout<<"pid_sibling"<<pid_sibling<<endl;
    return 0;
    
}
BTLeafNode::BTLeafNode()
{
    for (int i=0; i<70; i++) {
        key_rid[i].key=-1;
        key_rid[i].rid.pid=-1;
        key_rid[i].rid.sid=-1;
    }
    pid_sibling=-1;
    keycount=0;
    
}
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{
    RC rc;
    rc=pf.read(pid, buffer);
    char * tmp=buffer;
    memcpy(&keycount,tmp,sizeof(int));
    tmp=buffer+sizeof(keycount);
    for (int i=0; i<70; i++) {
        memcpy(&(key_rid[i].key),tmp,sizeof(int));
        tmp=tmp+sizeof(int);
        memcpy(&(key_rid[i].rid.pid),tmp,sizeof(int));
        tmp=tmp+sizeof(int);
        memcpy(&(key_rid[i].rid.sid),tmp,sizeof(int));
        tmp=tmp+sizeof(int);
    }
    memcpy(&pid_sibling,tmp,sizeof(int));
    return rc;
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{
    RC rc;
    char * tmp=buffer;
    memcpy(tmp,&keycount,sizeof(int));
    tmp=buffer+sizeof(keycount);
    for (int i=0; i<70; i++) {
        memcpy(tmp,&(key_rid[i].key),sizeof(int));
        tmp=tmp+sizeof(int);
        memcpy(tmp,&(key_rid[i].rid.pid),sizeof(int));
        tmp=tmp+sizeof(int);
        memcpy(tmp,&(key_rid[i].rid.sid),sizeof(int));
        tmp=tmp+sizeof(int);
    }
    memcpy(tmp,&pid_sibling,sizeof(int));
    rc=pf.write(pid, buffer);
    return rc;
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{
    return keycount;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{
    RC rc;
    if (keycount==70) {
        rc=RC_NODE_FULL;
        return rc;
    }
    else if(keycount==0)
    {
        key_rid[keycount].key=key;
        key_rid[keycount].rid=rid;
        keycount++;
        return 0;
    }
    else
    {
        if (key_rid[keycount-1].key<key) {
            key_rid[keycount].key=key;
            key_rid[keycount].rid=rid;
            keycount++;
            return 0;
        }
        else{
            for (int i=0; i<keycount; i++) {
                if (key_rid[i].key>key) {
                    for (int n=keycount; n>i; n--) {
                        key_rid[n]=key_rid[n-1];
                    }
                    key_rid[i].key=key;
                    key_rid[i].rid=rid;
                    keycount++;
                    return 0;
                }
            }
            
        }
    }
    return rc;
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
    RC rc;
    key_rid_pair tmp;
    if (key<key_rid[35].key) {
        for (int i=35; i<70; i++) {
            tmp=key_rid[i];
            rc=sibling.insert(tmp.key, tmp.rid);
            key_rid[i].key=-1;
        }
        keycount=35;
        rc=insert(key, rid);
    }
    else
    {
        for (int i=36; i<70; i++) {
            tmp=key_rid[i];
            rc=sibling.insert(tmp.key, tmp.rid);
            key_rid[i].key=-1;
        }
        rc=sibling.insert(key, rid);
    }
    siblingKey=sibling.key_rid[0].key;
    keycount=36;
    return rc;
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
    RC rc;
    if (searchKey<key_rid[0].key) {
        eid=0;
        rc=RC_NO_SUCH_RECORD;
        return rc;
    }
    if (searchKey>key_rid[keycount-1].key) {
        if (keycount==70) {
            eid=69;
            rc=RC_NO_SUCH_RECORD;
            return rc;
        }
        else
        {
            eid=keycount;
            rc=RC_NO_SUCH_RECORD;
            return rc;
        }
        
    }
    for (int i=0; i<keycount; i++) {
        if (searchKey==key_rid[i].key) {
            eid=i;
            return 0;
        }
    }
    for (int i=0; i<keycount-1; i++) {
        if ((searchKey>key_rid[i].key)&& (searchKey<key_rid[i+1].key)){
                eid=i+1;
                rc=RC_NO_SUCH_RECORD;
                return rc;
        }
    }
    return rc;
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
    key=key_rid[eid].key;
    rid=key_rid[eid].rid;
    return 0;
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{
    return pid_sibling;
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{
    pid_sibling=pid;
    return 0;
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
//void BTNonLeafNode:: Setkeycount(int n)
//{
//    keycount=n;
//}

RC BTNonLeafNode::print()
{
    cout<<"------------"<<endl;
    cout<<"This is a non-leaf"<<endl;
    for (int i=0; i<keycount; i++) {
        cout<<"item"<<i<<'\t'<<"pid="<<pids[i]<<'\t'<<"keys="<<keys[i]<<endl;
    }
    cout<<"last pid"<<'\t'<<pids[keycount]<<endl;
    cout<<"keycount"<<'\t'<<keycount<<endl;
    return 0;
    
}
BTNonLeafNode::BTNonLeafNode()
{
    keycount=0;
//    parent=-1;
    for (int i=0; i<70; i++) {
        keys[i]=-1;
        pids[i]=-1;
    }
    pids[70]=-1;
}
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{
    pf.read(pid, buffer);
    char * tmp=buffer;
    memcpy(&keycount,tmp, sizeof(int));
    tmp=buffer+sizeof(keycount);
    for (int i=0; i<70; i++) {
        memcpy(&(keys[i]),tmp,sizeof(int));
        tmp=tmp+sizeof(int);
    }
    for (int i=0; i<71; i++) {
        memcpy(&(pids[i]),tmp,sizeof(int));
        tmp=tmp+sizeof(int);
    }
//    memcpy(&parent,tmp,sizeof(int));
    return 0;
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{
    char * tmp=buffer;
    memcpy(tmp,&keycount,sizeof(int));
    tmp=buffer+sizeof(keycount);
    for (int i=0; i<70; i++) {
        memcpy(tmp,&(keys[i]),sizeof(int));
        tmp=tmp+sizeof(int);
    }
    for (int i=0; i<71; i++) {
        memcpy(tmp,&(pids[i]),sizeof(int));
        tmp=tmp+sizeof(int);
    }
//    memcpy(tmp,&parent,sizeof(int));
    pf.write(pid, buffer);
    return 0;
}
RC BTNonLeafNode:: setfirstpid(PageId pid)
{
    pids[0]=pid;
    return 0;
}
/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{ return keycount; }


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{
    RC rc;
    if (keycount==70) {
        rc=RC_NODE_FULL;
        return rc;
    }
    else
    {
        if (keys[keycount-1]<key) {
            keys[keycount]=key;
            pids[keycount+1]=pid;
            keycount++;
            return 0;
        }
        else{
            for (int i=0; i<keycount; i++) {
                if (keys[i]>key) {
                    for (int n=keycount; n>i; n--) {
                        pids[n+1]=pids[n];
                        keys[n]=keys[n-1];
                    }
                    keys[i]=key;
                    pids[i+1]=pid;
                    keycount++;
                    return 0;
                }
            }
            
        }
    }
    return rc;
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
    RC rc=0;
    int tmpkey;
    PageId tmppid;
    if (key<keys[34]) {
        sibling.setfirstpid(pids[35]);
        for (int i=35; i<70; i++) {
            tmpkey=keys[i];
            tmppid=pids[i+1];
            rc=sibling.insert(tmpkey, tmppid);
            keys[i]=-1;
        }
        keycount=35;
        rc=insert(key, pid);
        midKey=keys[35];
        keys[35]=-1;
    }
    else if ((key>keys[34])&&(key<keys[35]))
    {
        sibling.setfirstpid(pid);
        for (int i=35; i<70; i++) {
            tmpkey=keys[i];
            tmppid=pids[i+1];
            rc=sibling.insert(tmpkey, tmppid);
            keys[i]=-1;
        }
        midKey=key;
    }
    else{
        sibling.setfirstpid(pids[36]);
        for (int i=36; i<70; i++) {
            tmpkey=keys[i];
            tmppid=pids[i+1];
            rc=sibling.insert(tmpkey, tmppid);
            keys[i]=-1;
        }
        rc=sibling.insert(key,pid);
        midKey=keys[35];
        keys[35]=-1;
        
    }
    keycount=35;
    return rc;
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
    if (keys[0]>searchKey) {
        pid=pids[0];
        return 0;
    }
    for (int i=1; i<keycount; i++) {
        if ((keys[i-1]<=searchKey)&&(keys[i]>searchKey)) {
            pid=pids[i];
            return 0;
        }
    }
    pid=pids[keycount];
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
    pids[0]=pid1;
    keys[0]=key;
    pids[1]=pid2;
    keycount=1;
    return 0;
}
