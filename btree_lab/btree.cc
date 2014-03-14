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
	newsuperblock.info.parentnode=0;

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
	newrootnode.info.parentnode=0;

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


ERROR_T BTreeIndex::Insert_NotFull(const SIZE_T offset,
					   const KEY_T &key,
					   const VALUE_T &value)
{
	return ERROR_INSANE;
}

ERROR_T BTreeIndex::Insert_Full(const SIZE_T offset,
					   const KEY_T &key,
					   const VALUE_T &value,
					   const SIZE_T &node)
{

	return ERROR_INSANE;
}

ERROR_T BTreeIndex::InsertInternal(const SIZE_T &node,
					   const BTreeOp op,
					   const KEY_T &key,
					   const VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;
  KeyValuePair kvpair(key,value); //syntax for use would be b.SetKeyVal(&kvpair)

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
	return rc;
  }

  switch (b.info.nodetype) { 
		case BTREE_ROOT_NODE:			
		case BTREE_INTERIOR_NODE:
			// Treat rootnodes the same as interiornodes. find the correct ptr
			// Scan through key/ptr pairs
			//and recurse if possible
			for (offset=0;offset<b.info.numkeys;offset++) { 
				rc=b.GetKey(offset,testkey);
				if (rc) {  return rc; }
				if (key<testkey) {
			// OK, so we now have the first key that's larger
			// so we need to recurse on the ptr immediately previous to 
			// this one, if it exists
			rc=b.GetPtr(offset,ptr);
			if (rc) { return rc; }
			return InsertInternal(ptr,op,key,value);
			  }
			}
			// if we got here, we need to go to the next/last pointer, if it exists
			if (b.info.numkeys>0) { 
				rc=b.GetPtr(b.info.numkeys,ptr);
				if (rc) { return rc; }
				return InsertInternal(ptr,op,key,value);
			} else {
				// There are no keys at all on this node\
				// an internode should always have at least 1 key
				// This means we are on the first insert at the rootnode, and ROOT has no keys.
				// split the root into 2 leaves
				//Insert the new key
				b.info.numkeys = 1; //now it has one key(offset)
				rc=b.SetKey(0,key); //first key goes in the first offset.
				if (rc) {  return rc; }
				
				//Initialize the two new leaves
				SIZE_T leftleaf, rightleaf;
				//Now allocate the new nodes, save the ptr in the parent node, and set new node as leaf
				
				//Left Ptr
				rc=AllocateNode(leftleaf); //allocate the new block. new block will be at leftleaf
				if (rc) {  return rc; }
				rc=b.SetPtr(0,leftleaf); //Set the ptr in the rootnode. offset is 0.
				if (rc) {  return rc; }
				//Right Ptr	
				rc=AllocateNode(rightleaf); //allocate the new block. new block will be at rightleaf
				if (rc) {  return rc; }
				rc=b.SetPtr(1,rightleaf); //Set the ptr in the rootnode. offset is 1.
				if (rc) {  return rc; }
				//Serialize Root
				rc=b.Serialize(buffercache,node); //Since the ptrs have been created and added to rootnode
				if (rc) {  return rc; }
				
				//Left Leaf
				BTreeNode newleftnode; //initialize the left node
				rc=newleftnode.Unserialize(buffercache,leftleaf); //fill in the node from cache at the left block
				if (rc) {  return rc; }
				newleftnode.info.nodetype=BTREE_LEAF_NODE; //set it to a leaf node
				newleftnode.info.parentnode=node;
				rc=newleftnode.Serialize(buffercache,leftleaf); //save and close the node
				if (rc) {  return rc; }
				//Right Leaf				
				BTreeNode newrightnode; //initialize the right node
				rc=newrightnode.Unserialize(buffercache,rightleaf); //fill in the node from cache at the block which we just allocated
				if (rc) {  return rc; }
				newrightnode.info.nodetype=BTREE_LEAF_NODE; //set it to a leaf node
				newrightnode.info.parentnode=node;
				return InsertInternal(rightleaf,op,key,value);
				// it will recurse and add the new value to the end of the leaf we just created.
				
	      //other option would be to just manually set the key/val here, but if InsertInternal works properly, this should not be needed
				// rc=newleftnode.SetKey(0,key) //first key entry, offset=0
				// if (rc) {  return rc; }
				// rc=newleftnode.SetVal(0,value) //first key entry, offset=0
				// if (rc) {  return rc; }
				// rc=newrightnote.Serialize(buffercache,rightleaf);
				// if (rc) {  return rc; }
			}
			break;
		case BTREE_LEAF_NODE:
			// Scan through keys looking for matching value
			for (offset=0;offset<b.info.numkeys;offset++) { 
				rc=b.GetKey(offset,testkey);
				if (rc) {  return rc; }
				if (key<testkey) { // if there exists a key that is greater than the new key
					if (b.info.numkeys < 2*b.info.GetNumSlotsAsLeaf/3) { //if not 2/3rds full
						Insert_NotFull(offset,key,value); //function to insert into a leaf that is not full
					} else {
						Insert_Full(offset,key,value,node); //function to insert into a full leaf, with splitting
					}
				} else if (testkey==key) { //if the key already exists
					return ERROR_CONFLICT; // it is an error for an insert
				}
			}
			//if we get here, then none of the existing keys in the leaf need to be shifted
			//check if it is full, and insert at the end
			if (b.info.numkeys < 2*b.info.GetNumSlotsAsLeaf/3) { //if not 2/3rds full
			// offset=b.info.numkeys since we are at the end of the existing keys
				Insert_NotFull(b.info.numkeys,key,value); //function to insert into leaf that is not full
			} else {
				Insert_Full(b.info.numkeys,key,value,node); //function to insert into a full leaf, with splitting
			}
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
  return InsertInternal(superblock.info.rootnode, BTREE_OP_INSERT, key, value);
}

// node is the block number of the current node (parent)
// ptr is the block number of the node that needs to be split (child)
// b is the unserialized current node (parent)
// offset is the index of the pointer to the currect node that needs to be split (child)
// split is the index of the split in child
// nodeType is the type of the child node
ERROR_T BTreeIndex::Split(SIZE_T &node, SIZE_T &ptr, BTreeNode &b, int offset, SIZE_T split, int &nodeType)
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
  for (int i=split; i<b.info.numkeys-1; i++)
  {
    for (int j=(unsigned)0; j<b.info.numkeys-split; i++)
    {
      KEY_T pKey;
      rc = child.GetKey(i, pKey);
      if (rc) { return rc; }
      rc = n.SetKey(j, pKey);
      if (rc) { return rc; }
    }
  }

  // add pointer from parent to new node
  SIZE_T nPtr;
  KEY_T nKey;
  nPtr = (SIZE_T)n.ResolvePtr(0);
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




