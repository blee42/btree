#include <assert.h>
#include <iostream>
#include <sstream>
#include <string>
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
    //return ERROR_UNIMPL;
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
					   const VALUE_T &value,					   
					   const SIZE_T &nodenum,
					   BTreeNode &b)
{
  
  ERROR_T rc;
  KEY_T temp_key;
  VALUE_T temp_val;
  SIZE_T temp_offset;

  assert(b.info.nodetype == BTREE_LEAF_NODE); //Insert_NotFull should only be called at a leaf node
	b.info.numkeys++;
  //First step is to shift all values from offset to the right
	for (temp_offset=(b.info.numkeys-1);temp_offset>offset;temp_offset--) {
		//obtain key and value starting from the rightmost kvpair
		rc = b.GetKey(temp_offset-1,temp_key);
		if (rc) {  return rc; }
		rc = b.GetVal(temp_offset-1,temp_val);
		if (rc) {  return rc; }
		//save the key/value into the next slot (which should be open since leaf not full)
		rc = b.SetKey(temp_offset,temp_key);
		if (rc) {  return rc; }
		rc = b.SetVal(temp_offset,temp_val);
		if (rc) {  return rc; }
	}
	//all offset+1 pairs should now be right shifted
	rc = b.SetKey(offset,key);//insert new pair into leaf
	if (rc) {  return rc; }
	rc = b.SetVal(offset,value);
	if (rc) {  return rc; }
	return b.Serialize(buffercache,nodenum);
}

ERROR_T BTreeIndex::Insert_NotFullParent(const SIZE_T newnode,
					   const KEY_T &key,
					   BTreeNode &b,
					   const SIZE_T nodenum)
{

  ERROR_T rc;
  SIZE_T temp_offset;
  SIZE_T temp_ptr;
  KEY_T temp_key;
  SIZE_T offset;
  KEY_T testkey;
  
  assert(b.info.nodetype == BTREE_INTERIOR_NODE || b.info.nodetype == BTREE_ROOT_NODE); //Insert_FullParent should only be called on interior nodes and root nodes.
  
  for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey) {
	// OK, so we now have the first key that's larger
	// so we need to recurse on the ptr immediately previous to 
	// this one, if it exists
		break;
	  }
	}
	
	b.info.numkeys++;
	//First step is to shift all values from offset to the right
	for (temp_offset=(b.info.numkeys-1);temp_offset>offset;temp_offset--) {
		//obtain key and value starting from the rightmost kvpair
		rc = b.GetKey(temp_offset-1,temp_key);
		if (rc) {  return rc; }
		rc = b.GetPtr(temp_offset,temp_ptr);
		if (rc) {  return rc; }
		//save the key/value into the next slot (which should be open since leaf not full)
		rc = b.SetKey(temp_offset,temp_key);
		if (rc) {  return rc; }
		rc = b.SetPtr(temp_offset+1,temp_ptr);
		if (rc) {  return rc; }
	}
	//all offset+1 pairs should now be right shifted
	rc = b.SetKey(offset,key);//insert new pair into leaf
	if (rc) {  return rc; }
	rc = b.SetPtr(offset+1,newnode);
	if (rc) {  return rc; }
	return b.Serialize(buffercache,nodenum);
}

ERROR_T BTreeIndex::Insert_FullParent(const SIZE_T &newnode,
					   const KEY_T &key,
					   BTreeNode &b,
					   SIZE_T &nodenum)
{

  ERROR_T rc;
  SIZE_T temp_offset;
  SIZE_T temp_ptr;
  KEY_T temp_key;
  SIZE_T offset;
  KEY_T testkey;
  
  assert(b.info.nodetype == BTREE_INTERIOR_NODE || b.info.nodetype == BTREE_ROOT_NODE); //Insert_FullParent should only be called on interior nodes and root nodes.
  
  
  for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey) {
	// OK, so we now have the first key that's larger
	// so we need to recurse on the ptr immediately previous to 
	// this one, if it exists
		break;
	  }
	}

	b.info.numkeys++;
  //First step is to shift all values from offset to the right
	for (temp_offset=(b.info.numkeys-1);temp_offset>offset;temp_offset--) {
		//obtain key and value starting from the rightmost kvpair
		rc = b.GetKey(temp_offset-1,temp_key);
		if (rc) {  return rc; }
		rc = b.GetPtr(temp_offset,temp_ptr);
		if (rc) {  return rc; }
		//save the key/value into the next slot (which should be open since leaf not full)
		rc = b.SetKey(temp_offset,temp_key);
		if (rc) {  return rc; }
		rc = b.SetPtr(temp_offset+1,temp_ptr);
		if (rc) {  return rc; }
	}
	//all offset+1 pairs should now be right shifted
	rc = b.SetKey(offset,key);//insert new pair into leaf
	if (rc) {  return rc; }
	rc = b.SetPtr(offset+1,newnode);
	if (rc) {  return rc; }

	rc = Split(nodenum,b,temp_ptr,temp_key);
	if (rc) {  return rc; }
 	BTreeNode parent;
 	parent.Unserialize(buffercache,b.info.parentnode);
 	if (parent.info.numkeys < (2*b.info.GetNumSlotsAsInterior()/3)) {
 	//if (parent.info.numkeys < 4) {
 		return Insert_NotFullParent(temp_ptr,temp_key,parent,b.info.parentnode);
 	} else {
 		return Insert_FullParent(temp_ptr,temp_key,parent,b.info.parentnode);
	}
}  

ERROR_T BTreeIndex::Insert_Full(const SIZE_T offset,
					   const KEY_T &key,
					   const VALUE_T &value,
					   SIZE_T &nodenum,
					   BTreeNode &b)
{
  ERROR_T rc;
  SIZE_T temp_offset;
  KEY_T temp_key;
  SIZE_T temp_ptr;
  VALUE_T temp_val;

  // PSEUDOCODE
	b.info.numkeys++;
  //First step is to shift all values from offset to the right
	for (temp_offset=(b.info.numkeys-1);temp_offset>offset;temp_offset--) {
		//obtain key and value starting from the rightmost kvpair
		rc = b.GetKey(temp_offset-1,temp_key);
		if (rc) {  return rc; }
		rc = b.GetVal(temp_offset-1,temp_val);
		if (rc) {  return rc; }
		//save the key/value into the next slot (which should be open since leaf not full)
		rc = b.SetKey(temp_offset,temp_key);
		if (rc) {  return rc; }
		rc = b.SetVal(temp_offset,temp_val);
		if (rc) {  return rc; }
	}
	//all offset+1 pairs should now be right shifted
	rc = b.SetKey(offset,key);//insert new pair into leaf
	if (rc) {  return rc; }
	rc = b.SetVal(offset,value);
	if (rc) {  return rc; }
	rc=Split(nodenum,b,temp_ptr,temp_key);
	if (rc) {  return rc; }

 	BTreeNode parent;
 	parent.Unserialize(buffercache,b.info.parentnode);
 	if (parent.info.numkeys < (2*b.info.GetNumSlotsAsInterior()/3)) {
 	//if (parent.info.numkeys < 4) {
 		return Insert_NotFullParent(temp_ptr,temp_key,parent,b.info.parentnode);
 	} else {
 		return Insert_FullParent(temp_ptr,temp_key,parent,b.info.parentnode);
	}
}  



ERROR_T BTreeIndex::InsertInternal(SIZE_T &nodenum,
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

  rc= b.Unserialize(buffercache,nodenum);

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
				// There are no keys at all on this node
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
				rc=b.Serialize(buffercache,nodenum); //Since the ptrs have been created and added to rootnode
				if (rc) {  return rc; }
				
				//Left Leaf
				BTreeNode newleftnode(BTREE_LEAF_NODE,
					b.info.keysize,
					b.info.valuesize,
					buffercache->GetBlockSize()); //initialize the left node
				if (rc) {  return rc; }
				//fill it with what we want
				newleftnode.info.rootnode=b.info.rootnode;
				newleftnode.info.parentnode=nodenum;
				newleftnode.info.numkeys=0;
				rc=newleftnode.Serialize(buffercache,leftleaf); //save and close the node
				if (rc) {  return rc; }
				//Right Leaf				
				BTreeNode newrightnode(BTREE_LEAF_NODE,
					b.info.keysize,
					b.info.valuesize,
					buffercache->GetBlockSize()); //initialize the right node
				if (rc) {  return rc; }
				//fill it with what we want
				newrightnode.info.rootnode=b.info.rootnode;
				newrightnode.info.parentnode=nodenum;
				newrightnode.info.numkeys=0;
				rc=newrightnode.Serialize(buffercache,rightleaf); //save and close the node
				if (rc) {  return rc; }
				return InsertInternal(rightleaf,op,key,value);
        
				// it will recurse and add the new value to the end of the leaf we just created.
				
			}
			break;
		case BTREE_LEAF_NODE:
			// Scan through keys looking for matching value
			for (offset=0;offset<b.info.numkeys;offset++) { 
				rc=b.GetKey(offset,testkey);
				if (rc) {  return rc; }
				if (key<testkey) { // if there exists a key that is greater than the new key
					if (b.info.numkeys < (2*b.info.GetNumSlotsAsLeaf()/3)) { //if not 2/3rds full
					//if (b.info.numkeys < 4) { //if not test full
						return Insert_NotFull(offset,key,value,nodenum,b); //function to insert into a leaf that is not full
					} else {
						return Insert_Full(offset,key,value,nodenum,b); //function to insert into a full leaf, with splitting
					}
				} else if (testkey==key) { //if the key already exists
					return ERROR_CONFLICT; // it is an error for an insert
				}
			}
			//if we get here, then none of the existing keys in the leaf need to be shifted
			//check if it is full, and insert at the end
			if (b.info.numkeys < (2*b.info.GetNumSlotsAsLeaf()/3)) { //if not 2/3rds full
			//if (b.info.numkeys < 4) { //if not test full
			// offset=b.info.numkeys since we are at the end of the existing keys
				return Insert_NotFull(b.info.numkeys,key,value,nodenum,b); //function to insert into leaf that is not full
			} else {
				return Insert_Full(b.info.numkeys,key,value,nodenum,b); //function to insert into a full leaf, with splitting
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

ERROR_T BTreeIndex::Split(SIZE_T &nodenum,
             BTreeNode &b,
             SIZE_T &newNode,
             KEY_T &mid)
{
  ERROR_T rc;
  SIZE_T middle;

  // find middle split index
  middle = b.info.numkeys/2;
    // save the first key on the new node as mid
  rc = b.GetKey(middle,mid);

  // create new node
  rc = AllocateNode(newNode);
  if (rc) { return rc; }
  BTreeNode n(b.info.nodetype, b.info.keysize, b.info.valuesize, buffercache->GetBlockSize());
  n.info.rootnode=b.info.rootnode;
  n.info.parentnode=b.info.parentnode;
  n.info.numkeys=0;
  //make the newleftNode to be used if case is root
  SIZE_T newleftNode;
  if (b.info.nodetype == BTREE_ROOT_NODE) {
	rc = AllocateNode(newleftNode);
	if (rc) { return rc; }	
	n.info.nodetype = BTREE_INTERIOR_NODE;
	n.info.parentnode=b.info.rootnode; 
  }
  
  BTreeNode nL(BTREE_INTERIOR_NODE, b.info.keysize, b.info.valuesize, buffercache->GetBlockSize());
	nL.info.rootnode=b.info.rootnode;
	nL.info.parentnode=b.info.rootnode;
	nL.info.numkeys=0;
	
  switch(b.info.nodetype)
  {
    case BTREE_LEAF_NODE:
	  
      // copy half of the keys and values to new node
      for (unsigned int i=middle; i<b.info.numkeys; i++)
      {
        // incremember the number of keys in new node
        n.info.numkeys++;
        KEY_T cKey;
        VALUE_T cVal;

        rc = b.GetKey(i, cKey);
        if (rc) { return rc; }
        rc = n.SetKey(i-middle, cKey);
        if (rc) { return rc; }
        rc = b.GetVal(i, cVal);
        if (rc) { return rc; }
        rc = n.SetVal(i-middle, cVal);
        if (rc) { return rc; }
      }
      // set new number of keys in child
      b.info.numkeys=middle;
	  n.info.numkeys=middle+1;
      break;
	case BTREE_ROOT_NODE:
		//make the newleftNode, nL
		  if (rc) { return rc; }
		// copy all of the root values into the new left node
      for (unsigned int i=0; i<=b.info.numkeys; i++)
      {
        // increment the number of keys in new node
        nL.info.numkeys++;
        KEY_T cKey;
        SIZE_T cPtr;
		
		rc = b.GetPtr(i, cPtr);
        if (rc) { return rc; }
        rc = nL.SetPtr(i, cPtr);
        if (rc) { return rc; }
		if (i < b.info.numkeys) {
        rc = b.GetKey(i, cKey);
        if (rc) { return rc; }
        rc = nL.SetKey(i, cKey);
        if (rc) { return rc; }
		}
        
		
		BTreeNode temp;
		rc = temp.Unserialize(buffercache,cPtr);
        if (rc) { return rc; }
		temp.info.parentnode=newleftNode;
		rc = temp.Serialize(buffercache,cPtr);
        if (rc) { return rc; }
      }
	  b.info.numkeys=0; //since root is copied, set root numkeys to 0
	  rc = b.SetPtr(0,newleftNode); //set the first ptr to point to the new left new node
        if (rc) { return rc; }
	  rc = b.Serialize(buffercache,nodenum);
        if (rc) { return rc; }
	  rc = nL.Serialize(buffercache,newleftNode);
        if (rc) { return rc; }
	  //After writing root, we set the newleftNode to be the node we are splitting
	  nodenum=newleftNode;
	  rc = b.Unserialize(buffercache,nodenum); //this should now set b as the new left node
        if (rc) { return rc; }
		//after the increase in height, treat the split like a normal node.gdb
    case BTREE_INTERIOR_NODE:
      // copy half of the keys and pointers to new node
      for (unsigned int i=middle; i<=b.info.numkeys; i++)
      {
        // increment the number of keys in new node
        n.info.numkeys++;
        KEY_T cKey;
        SIZE_T cPtr;
		
		rc = b.GetPtr(i, cPtr);
        if (rc) { return rc; }
        rc = n.SetPtr(i-middle, cPtr);
        if (rc) { return rc; }
		
		if (i < b.info.numkeys) {
        rc = b.GetKey(i, cKey);
        if (rc) { return rc; }
        rc = n.SetKey(i-middle, cKey);
        if (rc) { return rc; }
		}
        
		
		BTreeNode temp;
		rc = temp.Unserialize(buffercache,cPtr);
        if (rc) { return rc; }
		temp.info.parentnode=newNode;
		rc = temp.Serialize(buffercache,cPtr);
        if (rc) { return rc; }
      }
      // set new number of keys in child
      b.info.numkeys=middle;
	  n.info.numkeys=middle+1;
      break;
    default:
		assert(0==1);
      return ERROR_INSANE;
	  break;
  }

  if (rc) { return rc; }

  // save changes to disk
  rc = n.Serialize(buffercache, newNode);
  if(rc){return rc;}
  return b.Serialize(buffercache, nodenum);
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
  SIZE_T root;

  // get pointer to root node
  root = superblock.info.rootnode;

  return SanityCheckInternal(root, 0);
  // return ERROR_UNIMPL;
}

ERROR_T BTreeIndex::SanityCheckInternal(SIZE_T nodenum,
         int prev) const
{
  ERROR_T rc;
  BTreeNode b;
  BTreeNode next;

  // unserialize the current node
  rc = b.Unserialize(buffercache, nodenum);
  if (rc) { return rc; }
  
  // check for cycles
  if (b.info.check)
  {
    // then this node has already been checked, indicating a cycle
    return ERROR_INSANE;
  }
  else 
  {
    b.info.check = true;
  }

  // traverse the tree
  for (unsigned int i=0; i<=b.info.numkeys; i++)
  {
    // check the node type of the pointer
    SIZE_T ptr;
    rc = b.GetPtr(i,ptr);
    if (rc) { return rc; }
    rc = next.Unserialize(buffercache, ptr);
    if (rc) { return rc; }

    // check parent node
    assert(nodenum == next.info.parentnode);

    if (next.info.nodetype!=BTREE_LEAF_NODE)
    {
      rc = SanityCheckInternal(ptr, prev);
      if (rc) { return rc; }
    }
    // the node is a leaf
    else
    {
      for (unsigned int j=0; j<next.info.numkeys; j++)
      {
        // get key at leaf
        VALUE_T curKey;
        rc = next.GetKey(j, curKey);
        if (rc) { return rc; }

        // extract integer from curKey and prevKey
        int curr;
        stringstream concatC;

        for (unsigned int k=0; k<curKey.length; k++)
        {
          concatC << curKey.data[k];
        }
        concatC >> curr;

        // check that if current val is smaller than previous
        if (curr < prev)
        {
          // violation of BTree property
          return ERROR_INSANE;
        }
        else {
          // set new prev key
          prev = curr;
        }
      }
    }
  }
  return rc;
}

ostream & BTreeIndex::Print(ostream &os) const
{
  Display(os, BTREE_DEPTH_DOT);
  return os;
}




