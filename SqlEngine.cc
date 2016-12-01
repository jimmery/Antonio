/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <stdlib.h>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <climits>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"

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

  vector<SelCond> remaining_conds;
  RC     rc;
  int    key;     
  string value;
  int    count;
  int    diff;

  int min_key;
  int max_key;
  bool conflicting_conditions;

  IndexCursor ic;
  BTLeafNode leaf;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    return rc;
  }

  // open the BTreeIndex. 
  BTreeIndex bt;
  rc = bt.open(table + ".idx", 'r');
  if (rc < 0) {
    goto read_all;
  }

  // the new stuff. if we do in fact find that there is an index. 
  // TODO figure out how to initialize these values? 

  // save the min/max value of the key (inclusive)
  min_key = INT_MIN;
  max_key = INT_MAX;
  conflicting_conditions = false;

  //=======================================
  // finding upper and lower bounds
  for (unsigned i = 0; i < cond.size(); i++)
  {
    SelCond sc = cond[i];
    if (sc.attr != 1) {
      remaining_conds.push_back(sc);
    } else if (sc.attr == 1) {
        // this flag indicates whether or not the condition was used to constrain
        // the min/max key bounds. This is useful for determining if we can 
        // remove the condition from the cond vector
        
        key = atoi(cond[i].value);
        switch(sc.comp) {
        case SelCond::EQ:
          if (key < min_key || key > max_key) {
              // constraint conflict. No need to check any more conditions
              conflicting_conditions = true;
              goto end_bounds_constraint;
          } else {
              min_key = key;
              max_key = min_key;
          }
          break;
        case SelCond::NE:
			    remaining_conds.push_back(sc);
          break;
        case SelCond::GT:
          if (max_key <= key) {
              // constraint conflict. No need to check any more conditions
              conflicting_conditions = true;
              goto end_bounds_constraint;
            }
            else if (min_key < key + 1)
              min_key = key + 1;
          break;
        case SelCond::LT:
            if (min_key >= key) {
                // constraint conflict. No need to check any more conditions
                conflicting_conditions = true;
                goto end_bounds_constraint;
            }
            else if (max_key > key - 1)
              max_key = key - 1;
            break;
        case SelCond::GE:
            if (max_key < key) {
                // constraint conflict. No need to check any more conditions
                conflicting_conditions = true;
                goto end_bounds_constraint;
            }
            else if (min_key < key)
              min_key = key;
            break;
        case SelCond::LE:
            if (min_key > key) {
                // constraint conflict. No need to check any more conditions
                conflicting_conditions = true;
                goto end_bounds_constraint;
            }
            else if (max_key > key)
              max_key = key;
            break;
        }
    }
  }
  
end_bounds_constraint:
  // if all conditions require a read of the full table, read_all
  if (remaining_conds.size() == cond.size())
	  goto read_all;
  if (conflicting_conditions)
    goto exit_select;

  rc = bt.locate(min_key, ic);
  //fprintf(stdout, "cursor.pid: %d, cursor.eid: %d\n", ic.pid, ic.eid);
  rc = bt.readForward(ic, key, rid);
  //fprintf(stdout, "cursor.pid: %d, cursor.eid: %d, key: %d\n", ic.pid, ic.eid, key);
  count = 0;

  while (!rc && (key <= max_key)) {
    for (unsigned i = 0; i < remaining_conds.size(); i++) {
      // compute the difference between the tuple value and the condition value
      switch (remaining_conds[i].attr) {
      case 1:
        diff = key - atoi(remaining_conds[i].value);
        break;
      case 2:
        diff = strcmp(value.c_str(), remaining_conds[i].value);
        break;
      }

      // skip the leaf if any condition is not met
      switch (remaining_conds[i].comp) {
      case SelCond::EQ:
        if (diff != 0) goto next_leaf;
        break;
      case SelCond::NE:
        if (diff == 0) goto next_leaf;
        break;
      case SelCond::GT:
        if (diff <= 0) goto next_leaf;
        break;
      case SelCond::LT:
        if (diff >= 0) goto next_leaf;
        break;
      case SelCond::GE:
        if (diff < 0) goto next_leaf;
        break;
      case SelCond::LE:
        if (diff > 0) goto next_leaf;
        break;
      }
    }

    // the condition is met for the tuple. 
    // increase matching tuple counter
    count++;

    switch (attr) {
    case 1:  // SELECT key
      // do not need a read, since we have the key. 
      fprintf(stdout, "%d\n", key);
      break;
    case 2:  // SELECT value
      // do a read to get the value. 
      rf.read(rid, key, value);
      fprintf(stdout, "%s\n", value.c_str());
      break;
    case 3:  // SELECT *
      rf.read(rid, key, value);
      fprintf(stdout, "%d '%s'\n", key, value.c_str());
      break;
    }

    next_leaf:
    rc = bt.readForward(ic, key, rid); 
//    fprintf(stdout, "cursor.pid: %d, cursor.eid: %d, key: %d, iteration: %d\n", ic.pid, ic.eid, key, count);
  }
    
  // print matching tuple count if "select count(*)"
  if (attr == 4) {
    fprintf(stdout, "%d\n", count);
  }
  
  //TODO define an exit select? 
  return rc;

  
  //    DESTINATION OF GOTO
  //===================================================================
  //    OLD CODE!!!!!!!!!
  read_all: 

  // scan the table file from the beginning
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

  // close the table file and return
  exit_select:
  rf.close();
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  /* your code here */
  RC rc; 
  BTreeIndex bt;  

  fstream myfile;
  myfile.open(loadfile.c_str());
  if (!myfile.is_open()) 
    return RC_FILE_OPEN_FAILED; // something went wrong with the IO. 

  
  RecordFile rfile; 
  rc = rfile.open((table + ".tbl"), 'w');
  if (rc < 0)
    return rc;

  if (index)
  {
    rc = bt.open((table + ".idx"), 'w');
    if (rc < 0)
      return rc;
  }

  string line;
  int key;
  string value;
  RecordId rid; 
  while (getline(myfile, line))
  {
    rc = parseLoadLine(line, key, value);
    rfile.append(key, value, rid); 
    // figure out what to do with rid? is that for the index? 
    if (index)
      bt.insert(key, rid);
  }
  
  myfile.close();
  rfile.close();
  if (index)
    bt.close();
  return 0;
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
