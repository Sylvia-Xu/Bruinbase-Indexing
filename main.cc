/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeNode.h"
#include "BTreeIndex.h"
#include <iostream>

using namespace std;
int main()
{
//    SqlEngine::run(stdin);
//    BTreeIndex tree;
//    BTLeafNode leaf1,leaf2;
//    BTNonLeafNode root;
//    PageFile pf;
//    RC rc;
//    
//    RecordId rid;
//    int key;
//    string value;
//    IndexCursor cursor;
//    
//    
//    tree.open("test", 'w');
//    for (int i=0; i<2000; i++) {
//                        rid.pid=3*i;
//                        rid.sid=3*i;
//                rc=tree.insert(3*i, rid);
////        rc=tree.locate(i, cursor);
////        cout<<"insert:"<<i<<'\t'<<rc<<endl;
//    }
//    for (int i=0; i<2000; i++) {
//        rid.pid=3*i+2;
//        rid.sid=3*i+2;
//        rc=tree.insert(3*i+2, rid);
//        //        rc=tree.locate(i, cursor);
//        //        cout<<"insert:"<<i<<'\t'<<rc<<endl;
//    }
//    for (int i=0; i<2000; i++) {
//        rid.pid=3*i+1;
//        rid.sid=3*i+1;
//        rc=tree.insert(3*i+1, rid);
//        //        rc=tree.locate(i, cursor);
//        //        cout<<"insert:"<<i<<'\t'<<rc<<endl;
//    }
//    leaf1.read(1, tree.pf);
//    leaf1.print();
//    leaf2.read(2, tree.pf);
//    leaf2.print();
//    root.read(3, tree.pf);
//    root.print();
    
//    tree.close();
    BTreeIndex tree;
    BTLeafNode leaf;
    BTNonLeafNode nonleaf;
    PageFile pf;
    RC rc;
    
    RecordId rid;
    int key;
    string value;
    IndexCursor cursor;
//
//    
    tree.open("xlarge", 'r');
//    tree.readForward(cursor, key, rid);
//    cout<<"key:"<<key<<"rid:"<<rid.pid<<endl;
    key=INT_MIN;
    tree.locate(key, cursor);
    tree.readForward(cursor, key, rid);
//    cout<<"the"<<0<<"key is:"<<key<<'\t'<<rid.pid<<'\t'<<rid.sid<<endl;
    PageId pid;
    pid=cursor.pid;
    int i=0;
    
    for (int j=1; cursor.pid>=0&&key<INT_MAX; j++) {
        tree.readForward(cursor, key, rid);
        if (cursor.pid!=pid) {
            cout<<i<<endl;
            cout<<"next page"<<endl;
            cout<<"next pid="<<cursor.pid<<endl;
//            leaf.read(pid, tree.pf);
//            cout<<"number of keys"<<leaf.getKeyCount()<<endl;
            pid=cursor.pid;
            i++;
        }
//        cout<<"the"<<j<<"key is:"<<key<<'\t'<<rid.pid<<'\t'<<rid.sid<<endl;
    }
//    cout<<"total number of leaf:"<<tree.getKeycount()<<endl;
    
//    for (int i=0; i<5000; i++) {
//        key=i;
//        tree.locate(key, cursor);
//        tree.readForward(cursor, key, rid);
//        cout<<"the"<<i<<"key is:"<<key<<'\t'<<rid.pid<<'\t'<<rid.sid<<endl;
//        if (cursor.pid<0) {
//            cout<<"the next pid is"<<cursor.pid<<endl;
//        }
//    }
    
    tree.close();

//    return 0;

    return 0;
}
