/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
    RecordFile rf;   // RecordFile containing the table
    RecordId   rid;  // record cursor for table scanning
    BTreeIndex idx;

    RC     rc;
    int    key;
    string value;
    int    count;
    int    diff=0;
    bool   indexexisting=false;
    int    keycondnum=0;
    int    maxkey=0;
    int    minkey=0;
    int    keycount=0;
    IndexCursor cursor;
/*flags of  kinds of conditions:
 0 key-equal
 1 key-nonequal
 2 key-others
 3 value
 */

    int    *condition;
    int conditioncount[4]={0};
    condition =new int[cond.size()];
    
    for (int i=0; i<cond.size(); i++) {
        if (cond[i].attr==1) {
            keycondnum++;
            if (cond[i].comp==SelCond::EQ) {
                condition[i]=0;
                conditioncount[0]++;
            }
            else if (cond[i].comp==SelCond::NE)
            {
                condition[i]=1;
                conditioncount[1]++;
            }
            else
            {
                condition[i]=2;
                conditioncount[2]++;
            }
        }
        else
        {
            condition[i]=3;
            conditioncount[3]++;
        }
    }
  // open the table file
    if ((rc = rf.open(table + ".tbl", 'r')) < 0)
    {
        fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
        return rc;
    }
// do not need index
    if ((conditioncount[0]==0)&&(conditioncount[2]==0)&&((attr==2)||(attr==3)))
    {
        rid.pid = rid.sid = 0;
        count = 0;
        while (rid < rf.endRid()) {
            // read the tuple
            if ((rc = rf.read(rid, key, value)) < 0) {
                fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
                goto exit_select;
            }
            
            // check the conditions on the tuple
            for (unsigned i = 0; i < cond.size(); i++) {
                // compute the difference between the tuple value and the condition value
                switch (cond[i].attr) {
                    case 1:
                        diff = key - atoi(cond[i].value);
                        break;
                    case 2:
                        diff = strcmp(value.c_str(), cond[i].value);
                        break;
                }
                // skip the tuple if any condition is not met
                switch (cond[i].comp) {
                    case SelCond::EQ:
                        if (diff != 0) goto next_tuple;
                        break;
                    case SelCond::NE:
                        if (diff == 0) goto next_tuple;
                        break;
                    case SelCond::GT:
                        if (diff <= 0) goto next_tuple;
                        break;
                    case SelCond::LT:
                        if (diff >= 0) goto next_tuple;
                        break;
                    case SelCond::GE:
                        if (diff < 0) goto next_tuple;
                        break;
                    case SelCond::LE:
                        if (diff > 0) goto next_tuple;
                        break;
                }
            }
            
            // the condition is met for the tuple.
            // increase matching tuple counter
            count++;
            
            // print the tuple
            switch (attr) {
                case 1:  // SELECT key
                    fprintf(stdout, "%d\n", key);
                    break;
                case 2:  // SELECT value
                    fprintf(stdout, "%s\n", value.c_str());
                    break;
                case 3:  // SELECT *
                    fprintf(stdout, "%d '%s'\n", key, value.c_str());
                    break;
            }
            
            // move to the next tuple
        next_tuple:
            ++rid;
        }
        
        // print matching tuple count if "select count(*)"
        if (attr == 4) {
            fprintf(stdout, "%d\n", count);
        }
        rc = 0;
        return rc;
        
        // close the table file and return
    exit_select:
        rf.close();
        return rc;
    }
    else
    {
        // open the index file
        rc=idx.open(table, 'r');
        indexexisting=false;
        if (rc==0)
        {
            indexexisting=true;
            maxkey=idx.getmaxKey();
            minkey=idx.getminKey();
            keycount=idx.getKeycount();
        }
        else
        {
            indexexisting=false;
        }
        //if there does not exist index, just scan all the tuples
        if (!indexexisting) {
            rid.pid = rid.sid = 0;
            count = 0;
            while (rid < rf.endRid()) {
                // read the tuple
                if ((rc = rf.read(rid, key, value)) < 0) {
                    fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
                    goto exit_select1;
                }
                
                // check the conditions on the tuple
                for (unsigned i = 0; i < cond.size(); i++) {
                    // compute the difference between the tuple value and the condition value
                    switch (cond[i].attr) {
                        case 1:
                            diff = key - atoi(cond[i].value);
                            break;
                        case 2:
                            diff = strcmp(value.c_str(), cond[i].value);
                            break;
                    }
                    // skip the tuple if any condition is not met
                    switch (cond[i].comp) {
                        case SelCond::EQ:
                            if (diff != 0) goto next_tuple1;
                            break;
                        case SelCond::NE:
                            if (diff == 0) goto next_tuple1;
                            break;
                        case SelCond::GT:
                            if (diff <= 0) goto next_tuple1;
                            break;
                        case SelCond::LT:
                            if (diff >= 0) goto next_tuple1;
                            break;
                        case SelCond::GE:
                            if (diff < 0) goto next_tuple1;
                            break;
                        case SelCond::LE:
                            if (diff > 0) goto next_tuple1;
                            break;
                    }
                }
                
                // the condition is met for the tuple.
                // increase matching tuple counter
                count++;
                
                // print the tuple
                switch (attr) {
                    case 1:  // SELECT key
                        fprintf(stdout, "%d\n", key);
                        break;
                    case 2:  // SELECT value
                        fprintf(stdout, "%s\n", value.c_str());
                        break;
                    case 3:  // SELECT *
                        fprintf(stdout, "%d '%s'\n", key, value.c_str());
                        break;
                }
                
                // move to the next tuple
            next_tuple1:
                ++rid;
            }
            
            // print matching tuple count if "select count(*)"
            if (attr == 4) {
                fprintf(stdout, "%d\n", count);
            }
            rc = 0;
            return rc;
            
            // close the table file and return
        exit_select1:
            rf.close();
            return rc;
        }
        // need index
        //calcualte range
        for (int i=0; i<cond.size(); i++) {
            if (condition[i]==2) {
                if (cond[i].comp==SelCond::GT)
                {
                    if ((atoi(cond[i].value)+1)>minkey) {
                        minkey=(atoi(cond[i].value)+1);
                    }
                }
                else if (cond[i].comp==SelCond::LT)
                {
                    if ((atoi(cond[i].value)-1)<maxkey) {
                        maxkey=(atoi(cond[i].value)-1);
                    }
                }
                else if (cond[i].comp==SelCond::GE)
                {
                    if (atoi(cond[i].value)>minkey) {
                        minkey=atoi(cond[i].value);
                    }
                }
                else if (cond[i].comp==SelCond::LE)
                {
                    if (atoi(cond[i].value)<maxkey) {
                        maxkey=atoi(cond[i].value);
                    }
                }
            }
        }
        // figure out if the range is meaningful
        if (maxkey<minkey) {
            switch (attr)
            {
                case 4:
                    fprintf(stdout, "0\n");
                    break;
            }
            delete [] condition;
            idx.close();
            rf.close();
            return 0;
        }
        // decide whether to retrieve the tuples or not
        if ((attr==2)||(attr==3)||(conditioncount[3]>0))
        {
            // retreive
            // has key-equal condition?
            if (conditioncount[0]>0) {
                int equalkey=-1;
                int first;
                for (int i=0; i<cond.size(); i++)
                {
                    if (condition[i]==0)
                    {
                        equalkey=atoi(cond[i].value);
                        first=i;
                        break;
                    }
                }
                for (int i=first+1; i<cond.size(); i++)
                {
                    if (condition[i]==0)
                    {
                        if(equalkey!=atoi(cond[i].value))
                        {
                            switch (attr)
                            {
                                case 4:
                                    fprintf(stdout, "0\n");
                                    break;
                            }
                            delete [] condition;
                            idx.close();
                            rf.close();
                            return 0;
                        }
                    }
                }
                if ((equalkey>maxkey)||(equalkey<minkey)) {
                    switch (attr)
                    {
                        case 4:
                            fprintf(stdout, "0\n");
                            break;
                    }
                    delete [] condition;
                    idx.close();
                    rf.close();
                    return 0;
                }
                else
                {
                    // confilict between EQ and NE
                    for (int i=0; i<cond.size(); i++)
                    {
                        if (condition[i]==1)
                        {
                            if(equalkey==atoi(cond[i].value))
                            {
                                switch (attr)
                                {
                                    case 4:
                                        fprintf(stdout, "0\n");
                                        break;
                                }
                                delete [] condition;
                                idx.close();
                                rf.close();
                                return 0;
                            }
                        }
                    }
                    if (idx.locate(equalkey, cursor)==0) {
                        bool flag=true;
                        idx.readForward(cursor, key, rid);
                        rf.read(rid, key, value);
                        for (int i=0; i<cond.size(); i++) {
                            if (condition[i]==3) {
                                diff = strcmp(value.c_str(), cond[i].value);
                                switch (cond[i].comp) {
                                    case SelCond::EQ:
                                        if (diff != 0) flag=false;
                                        break;
                                    case SelCond::NE:
                                        if (diff == 0) flag=false;
                                        break;
                                    case SelCond::GT:
                                        if (diff <= 0) flag=false;
                                        break;
                                    case SelCond::LT:
                                        if (diff >= 0) flag=false;
                                        break;
                                    case SelCond::GE:
                                        if (diff < 0) flag=false;
                                        break;
                                    case SelCond::LE:
                                        if (diff > 0) flag=false;
                                        break;
                                }
                                
                            }
                        }
                        if (flag) {
                            switch (attr) {
                                case 1:  // SELECT key
                                    fprintf(stdout, "%d\n", key);
                                    break;
                                case 2:  // SELECT value
                                    fprintf(stdout, "%s\n", value.c_str());
                                    break;
                                case 3:  // SELECT *
                                    fprintf(stdout, "%d '%s'\n", key, value.c_str());
                                    break;
                                    break;
                                case 4:
                                    fprintf(stdout, "1\n");
                                break;
                            }
                            delete [] condition;
                            idx.close();
                            rf.close();
                            return 0;
                        }
                        else
                        {
                            switch (attr)
                            {
                                case 4:
                                    fprintf(stdout, "0\n");
                                    break;
                            }
                            delete [] condition;
                            idx.close();
                            rf.close();
                            return 0;
                            
                        }
                        
                    }
                    else
                    {
                        switch (attr)
                        {
                            case 4:
                                fprintf(stdout, "0\n");
                                break;
                        }
                        delete [] condition;
                        idx.close();
                        rf.close();
                        return 0;
                        
                    }
                    
                }
                
                
            }
            //
            else
            {
                count=0;
                rc=idx.locate(minkey, cursor);
                idx.readForward(cursor, key, rid);
                if (key>maxkey)
                {
                    switch (attr)
                    {
                        case 4:
                            fprintf(stdout, "0\n");
                            break;
                    }
                    delete [] condition;
                    idx.close();
                    rf.close();
                    return 0;
                }
                else
                {
                    bool flag=true;
//                    idx.readForward(cursor, key, rid);
                    rf.read(rid, key, value);
                    for (int i=0; i<cond.size(); i++) {
                        if (condition[i]==3) {
                            diff = strcmp(value.c_str(), cond[i].value);
                            switch (cond[i].comp) {
                                case SelCond::EQ:
                                    if (diff != 0) flag=false;
                                    break;
                                case SelCond::NE:
                                    if (diff == 0) flag=false;
                                    break;
                                case SelCond::GT:
                                    if (diff <= 0) flag=false;
                                    break;
                                case SelCond::LT:
                                    if (diff >= 0) flag=false;
                                    break;
                                case SelCond::GE:
                                    if (diff < 0) flag=false;
                                    break;
                                case SelCond::LE:
                                    if (diff > 0) flag=false;
                                    break;
                            }
                            
                        }
                    }
                    if (flag) {
                        count++;
                        switch (attr) {
                            case 1:  // SELECT key
                                fprintf(stdout, "%d\n", key);
                                break;
                            case 2:  // SELECT value
                                fprintf(stdout, "%s\n", value.c_str());
                                break;
                            case 3:  // SELECT *
                                fprintf(stdout, "%d '%s'\n", key, value.c_str());
                                break;
                        }
                    }
                    while (cursor.pid>=0) {
                        bool flag=true;
                        idx.readForward(cursor, key, rid);
                        if (key>maxkey) {
                            switch (attr) {
                                case 4:
                                    fprintf(stdout, "%d\n", count);
                                    break;
                                    
                            }
                            delete [] condition;
                            idx.close();
                            rf.close();
                            return 0;
                        }
                        else
                        {
                            for (int i=0; i<cond.size(); i++) {
                                if (condition[i]==1) {
                                    if (key==atoi(cond[i].value)) {
                                        flag=false;
                                    }
                                }
                            }
                            rf.read(rid, key, value);
                            for (int i=0; i<cond.size(); i++) {
                                if (condition[i]==3) {
                                    diff = strcmp(value.c_str(), cond[i].value);
                                    switch (cond[i].comp) {
                                        case SelCond::EQ:
                                            if (diff != 0) flag=false;
                                            break;
                                        case SelCond::NE:
                                            if (diff == 0) flag=false;
                                            break;
                                        case SelCond::GT:
                                            if (diff <= 0) flag=false;
                                            break;
                                        case SelCond::LT:
                                            if (diff >= 0) flag=false;
                                            break;
                                        case SelCond::GE:
                                            if (diff < 0) flag=false;
                                            break;
                                        case SelCond::LE:
                                            if (diff > 0) flag=false;
                                            break;
                                    }
                                    
                                }
                            }
                            if (flag) {
                                switch (attr) {
                                    case 1:  // SELECT key
                                        fprintf(stdout, "%d\n", key);
                                        break;
                                    case 2:  // SELECT value
                                        fprintf(stdout, "%s\n", value.c_str());
                                        break;
                                    case 3:  // SELECT *
                                        fprintf(stdout, "%d '%s'\n", key, value.c_str());
                                        break;
                                }
                                count++;
                            }
                        }
                    }
                    switch (attr) {
                        case 4:
                            fprintf(stdout, "%d\n", count);
                            break;
                            
                    }
                    delete [] condition;
                    idx.close();
                    rf.close();
                    return 0;
                }
                
                
            }
            
        }
        else
        {
            //not retreive
            // has key-equal condition?
            if (conditioncount[0]>0) {
                int equalkey=-1;
                int first;
                for (int i=0; i<cond.size(); i++)
                {
                    if (condition[i]==0)
                    {
                        equalkey=atoi(cond[i].value);
                        first=i;
                        break;
                    }
                }
                for (int i=first+1; i<cond.size(); i++)
                {
                    if (condition[i]==0)
                    {
                        if(equalkey!=atoi(cond[i].value))
                        {
                            switch (attr)
                            {
                                case 4:
                                    fprintf(stdout, "0\n");
                                    break;
                            }
                            delete [] condition;
                            idx.close();
                            rf.close();
                            return 0;
                        }
                    }
                }
                if ((equalkey>maxkey)||(equalkey<minkey)) {
                    switch (attr)
                    {
                        case 4:
                            fprintf(stdout, "0\n");
                            break;
                    }
                    delete [] condition;
                    idx.close();
                    rf.close();
                    return 0;
                }
                else
                {
                    // confilict between EQ and NE
                    for (int i=0; i<cond.size(); i++)
                    {
                        if (condition[i]==1)
                        {
                            if(equalkey==atoi(cond[i].value))
                            {
                                switch (attr)
                                {
                                    case 4:
                                        fprintf(stdout, "0\n");
                                        break;
                                }
                                delete [] condition;
                                idx.close();
                                rf.close();
                                return 0;
                            }
                        }
                    }
                    if (idx.locate(equalkey, cursor)==0) {
                        switch (attr)
                        {
                            case 1:  // SELECT key
                                fprintf(stdout, "%d\n", equalkey);
                                break;
                            case 4:
                                fprintf(stdout, "1\n");
                                break;
                        }
                        delete [] condition;
                        idx.close();
                        rf.close();
                        return 0;
                    }
                    else
                    {
                        switch (attr)
                        {
                            case 4:
                                fprintf(stdout, "0\n");
                                break;
                        }
                        delete [] condition;
                        idx.close();
                        rf.close();
                        return 0;
                        
                    }
                    
                }
                    
                    
            }
            //
            else
            {
                count=0;
                rc=idx.locate(minkey, cursor);
                idx.readForward(cursor, key, rid);
                if (key>maxkey)
                {
                    switch (attr)
                    {
                        case 4:
                            fprintf(stdout, "0\n");
                            break;
                    }
                    delete [] condition;
                    idx.close();
                    rf.close();
                    return 0;
                }
                else
                {
                    count++;
                    switch (attr) {
                        case 1:  // SELECT key
                            fprintf(stdout, "%d\n", key);
                            break;
                    }
                    while (cursor.pid>=0) {
                        bool flag=true;
                        idx.readForward(cursor, key, rid);
                        if (key>maxkey) {
                            switch (attr) {
                                case 4:
                                    fprintf(stdout, "%d\n", count);
                                    break;
                            
                            }
                            delete [] condition;
                            idx.close();
                            rf.close();
                            return 0;
                        }
                        else
                        {
                            for (int i=0; i<cond.size(); i++) {
                                if (condition[i]==1) {
                                    if (key==atoi(cond[i].value)) {
                                        flag=false;
                                    }
                                }
                            }
                            if (flag) {
                                switch (attr) {
                                    case 1:  // SELECT key
                                        fprintf(stdout, "%d\n", key);
                                        break;
                                }
                                count++;
                            }
                        }
                    }
                    switch (attr) {
                        case 4:
                            fprintf(stdout, "%d\n", count);
                            break;
                            
                    }
                    delete [] condition;
                    idx.close();
                    rf.close();
                    return 0;
                }
                
                
            }
            
        }
        
    }
         return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  /* your code here */
    RecordFile rf;   // RecordFile containing the table
    RecordId   rid;  // record cursor for table loading

    RC     rc;
    string s;
    int    key;
    string value;
    BTreeIndex idx;
    

    //open the load file
    ifstream myfile(loadfile.c_str());


    // open the table file
    if ((rc = rf.open(table + ".tbl", 'w')) < 0) {
        return rc;
    }
    if (index) {
        rc=idx.open(table, 'w');
    }
    
    while (getline(myfile,s)) {
        rid=rf.endRid();
        rc=parseLoadLine(s, key, value);
        rc=rf.append(key, value, rid);
        if (index)
        {
            rc=idx.insert(key, rid);
        }
        
    }
    
    rc=rf.close();
    if (index) {
        rc=idx.close();
    }
    myfile.close();

  return rc;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
