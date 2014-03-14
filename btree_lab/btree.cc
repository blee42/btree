#include <assert.h>
#include "btree.h"

KeyValuePair::KeyValuePair()
{}


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) : 
  key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
  key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize, 
		       SIZE_T valuesize,
		       BufferCache *cache,
		       bool unique) 
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now
}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) { 
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+2;
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) { 
      return rc;
    }
    
    BTreeNode newrootnode(BTREE_ROOT_NODE,
			  superblock.info.keysize,
			  superblock.info.valuesize,
			  buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) { 
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) { 
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;
      
      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
	return rc;
      }

    }
  }

  // OK, now, mounting the btree is simply a matter of reading the superblock 

  return superblock.Unserialize(buffercache,initblock);
}
    

ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}
 

ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
					   const BTreeOp op,
					   const KEY_T &key,
					   VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey) {
	// OK, so we now have the first key that's larger
	// so we ned to recurse on the ptr immediately previous to 
	// this one, if it exists
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	return LookupOrUpdateInternal(ptr,op,key,value);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return LookupOrUpdateInternal(ptr,op,key,value);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) { 
	if (op==BTREE_OP_LOOKUP) { 
	  return b.GetVal(offset,value);
	} else { 
	  rc=b.SetVal(offset,value);
    if(rc) { return rc; }
    return b.Serialize(buffercache, node);
    return ERROR_UNIMPL;
	}
      }
    }
    return ERROR_NONEXISTENT;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }  

  return ERROR_INSANE;
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;

  if (dt==BTREE_DEPTH_DOT) { 
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) { 
      } else { 
	os << "Interior: ";
      }
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	os << "*" << ptr << " ";
	// Last pointer
	if (offset==b.info.numkeys) break;
	rc=b.GetKey(offset,key);
	if (rc) {  return rc; }
	for (i=0;i<b.info.keysize;i++) { 
	  os << key.data[i];
	}
	os << " ";
      }
    }
    break;
  case BTREE_LEAF_NODE:
    if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) { 
    } else {
      os << "Leaf: ";
    }
    for (offset=0;offset<b.info.numkeys;offset++) { 
      if (offset==0) { 
	// special case for first pointer
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (dt!=BTREE_SORTED_KEYVAL) { 
	  os << "*" << ptr << " ";
	}
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << "(";
      }
      rc=b.GetKey(offset,key);
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) { 
	os << key.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ",";
      } else {
	os << " ";
      }
      rc=b.GetVal(offset,value);
      if (rc) {  return rc; }
      for (i=0;i<b.info.valuesize;i++) { 
	os << value.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ")\n";
      } else {
	os << " ";
      }
    }
    break;
  default:
    if (dt==BTREE_DEPTH_DOT) { 
      os << "Unknown("<<b.info.nodetype<<")";
    } else {
      os << "Unsupported Node Type " << b.info.nodetype ;
    }
  }
  if (dt==BTREE_DEPTH_DOT) { 
    os << "\" ]";
  }
  return ERROR_NOERROR;
}
  
ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}

ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
  // ERROR_T rc;
  // bool splitFlag;
  // SIZE_T middle;
  // int nodeType;

  // // Try InsertInternalNode first and handle other cases 
  // rc = InsertInternalNode( superblock.info.rootnode,splitFlag, middle,nodeType,  BTREE_OP_INSERT, key, value);
  // if(rc){return rc;}

  // //split the root
  // if(splitFlag) {
  //   SIZE_T n;
  //   AllocateNode(n);

  //   BTreeNode newRoot(BTREE_ROOT_NODE, superblock.info.keysize, superblock.info.valuesize, buffercache->GetBlockSize());

  //   // changes node type of root to interior node
  //   BTreeNode oldRoot;
  //   rc = oldRoot.Unserialize(buffercache, superblock.info.rootnode);
  //   oldRoot.info.nodetype = BTREE_INTERIOR_NODE;
  //   rc = oldRoot.Serialize(buffercache, superblock.info.rootnode);
  //   if(rc){return rc;}

  //   rc = newRoot.SetPtr(0, superblock.info.rootnode);
  //   if(rc) {return rc;}

  //   // splits the root node 
  //   rc = Split(splitFlag, superblock.info.rootnode, middle, newRoot, n, 0, nodeType);

  //   superblock.info.rootnode = n;

  // }

  // return rc;
  return ERROR_UNIMPL;
}

// ERROR_T BTreeIndex::InsertInternalNode(const SIZE_T &node, bool &splitFlag, SIZE_T &middle, int &nodeType, const BTreeOp op, const KEY_T &key, const VALUE_T &value)
// {
//   BTreeNode b;
//   ERROR_T rc;
//   SIZE_T offset;
//   KEY_T testkey;
//   SIZE_T ptr;

//   rc= b.Unserialize(buffercache,node);
//   SIZE_T maxCapacityLeaf = b.info.GetNumSlotsAsLeaf();

//   if (rc!=ERROR_NOERROR) { 
//     return rc;
//   }

//   switch (b.info.nodetype) { 
//   case BTREE_ROOT_NODE:
//     // Scan through key/ptr pairs
//     // and recurse if possible
//     for (offset=0;offset<b.info.numkeys;offset++) { 
//       rc=b.GetKey(offset,testkey);
//       if (rc) {  return rc; }
//       if (key<testkey) {
//           // OK, so we now have the first key that's larger
//           // so we need to recurse on the ptr immediately previous to 
//           // this one, if it exists
//           rc=b.GetPtr(offset,ptr);
//           if (rc) { return rc; }
//           rc = InsertInternalNode(ptr,splitFlag,middle,nodeType,op,key,value);
//           if(rc) {return rc;}
          
//           if(splitFlag) {
//             rc = Split(splitFlag, ptr, middle, b,node, offset, nodeType);
//             if(rc){return rc;}
//           }
//           else {
//           rc = b.Serialize(buffercache, node);
//           if(rc){return rc;}
//           }

//           return rc;
//       }
//     }
//     // if we got here, we need to go to the next pointer, if it exists
//     if (b.info.numkeys>0) { 
//       rc=b.GetPtr(b.info.numkeys,ptr);
//       if (rc) { return rc; }
//         rc = InsertInternalNode(ptr,splitFlag,middle,nodeType,op,key,value);
//         if(rc){return rc;}
//         if(splitFlag) {
//           rc = Split(splitFlag, ptr, middle, b,node, offset, nodeType);
//           if(rc){return rc;}
          
//         }
//         else {
//         rc = b.Serialize(buffercache, node);
//         }
//         return rc;

//     } else {

//   // Special case: we don't have keys in root node; need to handle separately  
//   // since we are treating the root node as an interior node, we have to create two nodes that it will point to. Then we copy the key into both the root node and newNode. Next we set rootnode to point to newnode on its first branch and emptyNode on its next.

//   SIZE_T newNode;

//   b.info.numkeys++;
//   AllocateNode(newNode);
//   BTreeNode n(BTREE_LEAF_NODE, b.info.keysize, b.info.valuesize, buffercache->GetBlockSize());
//   n.info.numkeys++;

//   SIZE_T emptyNode;
//   AllocateNode(emptyNode);
//   BTreeNode emptyN(BTREE_LEAF_NODE, b.info.keysize, b.info.valuesize, buffercache->GetBlockSize());

//   rc = n.SetKey(offset, key);
//   if(rc) { return rc;}
//   rc = n.SetVal(offset, value);
//   if(rc) { return rc;}
//   rc = b.SetKey(offset, key);
//   if(rc) { return rc;}
//   rc = b.SetPtr(offset, newNode);
//   if(rc) { return rc;}
//   rc = b.SetPtr(offset+1, emptyNode);
//   if(rc) { return rc;}
//   rc = b.Serialize(buffercache, node);
//   if(rc) { return rc;}
//   rc = n.Serialize(buffercache, newNode);
//   if(rc) { return rc;}
//   rc = emptyN.Serialize(buffercache, emptyNode);
//   if(rc) { return rc;}


//   return rc; 


//     }
//     break;
//   case BTREE_INTERIOR_NODE:
//     // Scan through key/ptr pairs
//     //and recurse if possible
//     for (offset=0;offset<b.info.numkeys;offset++) { 
//       rc=b.GetKey(offset,testkey);
//       if (rc) {  return rc; }
//       if (key<testkey || key==testkey) {
//   // OK, so we now have the first key that's larger
//   // so we ned to recurse on the ptr immediately previous to 
//   // this one, if it exists
//   rc=b.GetPtr(offset,ptr);
//   if (rc) { return rc; }
//   rc = InsertInternalNode(ptr,splitFlag,middle,nodeType,op,key,value);
//   if(rc){return rc;}
//   if(splitFlag) {
//     rc = Split(splitFlag, ptr, middle, b,node, offset, nodeType);
//     if(rc){return rc;}
//   }
//   else {
//   rc = b.Serialize(buffercache, node);
//   }
//   return rc;
//       }
//     }
//     // if we got here, we need to go to the next pointer, if it exists
//     if (b.info.numkeys>0) { 
//       rc=b.GetPtr(b.info.numkeys,ptr);
//       if (rc) { return rc; }
//       rc = InsertInternalNode(ptr,splitFlag,middle,nodeType,op,key,value);
//       if(rc){return rc;}
//       if(splitFlag) {
//         rc = Split(splitFlag, ptr, middle, b, node, offset, nodeType);
//         if(rc){return rc;}
//       }
//       else {
//       rc = b.Serialize(buffercache, node);
//       }
//       return rc;
//     } else {
//       return ERROR_INSANE;  
//     }
//     break;
//   case BTREE_LEAF_NODE:
//     // Scan through keys looking for matching value
//     for (offset=0;offset<b.info.numkeys;offset++) { 
//       rc=b.GetKey(offset,testkey);
//       if (rc) {  return rc; }
//       // If key is found, then don't allow the insert.
//       if (testkey==key) { 
//          return ERROR_CONFLICT;
//       }
//       if(key < testkey) { break; }
//     }

//     b.info.numkeys++;

//     // Shifts keys/values over based on where the insert slot is (determined by offset)
//     for(unsigned int i = b.info.numkeys-1; i >= offset + 1; i--) {
//       KEY_T copyKey;
//       VALUE_T copyVal;
//       rc = b.GetKey(i-1, copyKey);
//       if(rc){return rc;}
//       rc = b.SetKey(i, copyKey);
//       if(rc){return rc;}
//       rc = b.GetVal(i-1, copyVal);
//       if(rc){return rc;}
//       rc = b.SetVal(i, copyVal);
//       if(rc){return rc;}
//     }

//     // Inserts input key/value into the correct slot 
//     rc = b.SetKey(offset, key);
//     if(rc){return rc;}
//     rc = b.SetVal(offset, value);
//     if(rc){return rc;}

//     // Handles the case where the node that the key/value being inserted on is full 
//     if(b.info.numkeys == maxCapacityLeaf) {
//       splitFlag= true;
//       middle = (b.info.numkeys + 1) / 2;
//       nodeType = BTREE_LEAF_NODE;

//     }
//     else
//       splitFlag= false;

//     b.Serialize(buffercache, node);


//     return rc;
//     break;
//   default:
//     // We can't be looking at anything other than a root, internal, or leaf
//     return ERROR_INSANE;
//     break;
//   }  

//   return ERROR_INSANE;
// }

// node is the block number of the current node (parent)
// ptr is the block number of the node that needs to be split (child)
// b is the unserialized current node (parent)
// offset is the index of the pointer to the currect node that needs to be split (child)
// split is the index of the split in child
// nodeType is the type of the child node
ERROR_T BTreeIndex::Split(SIZE_T node, SIZE_T ptr, BTreeNode &b, int offset, SIZE_T split, int &nodeType)
{
  ERROR_T rc;
  SIZE_T newNode;
  BTreeNode child;

  // unserialize node that needs to be split (child)
  rc = child.Unserialize(buffercache, ptr);
  if (rc) { return rc; }

  // new node
  rc = AllocateNode(newNode);
  if (rc) { return rc; }
  BTreeNode n(nodeType, b.info.keysize, b.info.valuesize, buffercache->GetBlockSize());

  // copy second half of values from child to to new node
  for (i=split; i<b.info.numkeys-1; i++)
  {
    for (j=0; j<b.info.numkeys-middle; i++)
    {
      KEY_T pKey;
      rc = child.GetKey(i, pKey);
      if (rc) { return rc; }
      rc = n.SetKey(j, pKey);
      if (rc) { return rc; }
    }
  }

  // add pointer from parent to new node
  KEY_T nKey;
  nPtr = n.ResolvePtr(0)
  rc = b.SetPtr(offset+1, nPtr);
  if (rc) { return rc; }
  rc = n.GetKey(0, nKey);
  if (rc) { return rc; }
  rc = b.SetKey(offset+1, nKey);

  // save changes to disk
  rc = n.Serialize(buffercache, newNode);
  if(rc){return rc;}
  rc = child.Serialize(buffercache, ptr);
  if(rc){return rc;}
  rc = b.Serialize(buffercache, node);
  if(rc){return rc;}

  return rc;
}

// ERROR_T BTreeIndex::Split(bool &splitFlag, SIZE_T ptr, SIZE_T &middle, BTreeNode &b, SIZE_T node, int offset, int &nodeType)
// {
//   // Sets up variables for stepping through the Btree
//   ERROR_T rc;
//   SIZE_T newNode;
//   BTreeNode child;
//   SIZE_T maxCapacityInterior = b.info.GetNumSlotsAsInterior();

//   rc = child.Unserialize(buffercache, ptr);
//   if(rc) {return rc;}
//   KEY_T middleKey;
//   rc = child.GetKey(middle, middleKey);
//   if(rc) { return rc;}

//   b.info.numkeys++;

//   // Shifts key and pointers around to make space for the new node
//   for(int i = b.info.numkeys - 2; i >= offset; i--) {
//     KEY_T parentKey;
//     SIZE_T parentPtr;
//     rc = b.GetKey(i, parentKey);
//     if(rc) {return rc;}
//     rc = b.SetKey(i+1, parentKey);
//     if(rc) {return rc;}
//     rc = b.GetPtr(i+1, parentPtr);
//     if(rc) {return rc;}
//     rc = b.SetPtr(i+2, parentPtr);
//     if(rc) {return rc;}
//   }
  
//   // Creates new node for extra keys to be placed on during balancing
//   rc = AllocateNode(newNode);
//   if(rc){return rc;}
//   BTreeNode n(nodeType, b.info.keysize, b.info.valuesize, buffercache->GetBlockSize());
  

//   // Assigns key and ptr to the node that gets created from the split 
//   rc = b.SetKey(offset, middleKey);
//   if(rc){return rc;}
//   rc = b.SetPtr(offset+1, newNode);
//   if(rc){return rc;}

//   SIZE_T copyPtr;

//   //
//   // Handles node balancing depending on the type of node being split
//   //
//   switch(nodeType)
//   {
//     case BTREE_LEAF_NODE:
//       for(unsigned int i = middle+1; i < child.info.numkeys; i++) {
//         n.info.numkeys++;
//         KEY_T copyKey;
//         VALUE_T copyVal;

//         rc = child.GetKey(i, copyKey);
//         if(rc){return rc;}
//         rc = n.SetKey(i - middle - 1, copyKey);
//         if(rc) {return rc;}
//         rc = child.GetVal(i, copyVal);
//         if(rc){return rc;}
//         rc = n.SetVal(i - middle - 1, copyVal);
//         if(rc) {return rc;}
//       }
//       child.info.numkeys = middle+1;
//       break;
//     case BTREE_INTERIOR_NODE:
//       for(unsigned int i = middle+1; i < child.info.numkeys; i++) {
//         n.info.numkeys++;
//         KEY_T copyKey;

//         rc = child.GetKey(i, copyKey);
//         if(rc){return rc;}
//         rc = n.SetKey(i - middle - 1, copyKey);
//         if(rc) {return rc;}
//         rc = child.GetPtr(i, copyPtr);
//         if(rc){return rc;}
//         rc = n.SetPtr(i - middle - 1, copyPtr);
//         if(rc) {return rc;}
//       }
      
//       rc = child.GetPtr(child.info.numkeys, copyPtr);
//       if(rc){return rc;}
//       rc = n.SetPtr(child.info.numkeys - middle - 1, copyPtr);
//       if(rc){return rc;}
//       child.info.numkeys = middle+1;
//       break;
//     default:
//       return ERROR_INSANE;

//   }

//   // Saves changes to disk
//   rc = child.Serialize(buffercache, ptr);
//   if(rc){return rc;}
//   rc = n.Serialize(buffercache, newNode);
//   if(rc){return rc;}
//   rc = b.Serialize(buffercache, node);
//   if(rc){return rc;}

//   // Handles the case where the node that the key/value being inserted on is full 
//   if(b.info.numkeys == maxCapacityInterior) {
//      splitFlag= true;
//      middle = (b.info.numkeys + 1) / 2;
//      nodeType = BTREE_INTERIOR_NODE;
//    }
//    else
//      splitFlag= false;

//    return rc;

// }
  
ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  VALUE_T val = value;
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, val);  
  // return ERROR_UNIMPL;
}

  
ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
  // This is optional extra credit 
  //
  // 
  return ERROR_UNIMPL;
}

  
//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
				    ostream &o,
				    BTreeDisplayType display_type) const
{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  rc = PrintNode(o,node,b,display_type);
  
  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) { 
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0) { 
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (display_type==BTREE_DEPTH_DOT) { 
	  o << node << " -> "<<ptr<<";\n";
	}
	rc=DisplayInternal(ptr,o,display_type);
	if (rc) { return rc; }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    return ERROR_NOERROR;
    break;
  default:
    if (display_type==BTREE_DEPTH_DOT) { 
    } else {
      o << "Unsupported Node Type " << b.info.nodetype ;
    }
    return ERROR_INSANE;
  }

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "digraph tree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "}\n";
  }
  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheck() const
{
  // WRITE ME
  return ERROR_UNIMPL;
}
  


ostream & BTreeIndex::Print(ostream &os) const
{
  Display(os, BTREE_DEPTH_DOT);
  return os;
}




