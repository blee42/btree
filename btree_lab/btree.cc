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

ERROR_T BTreeIndex::Insert_Full(const SIZE_T offset,
					   const KEY_T &key,
					   const VALUE_T &value,
					   const SIZE_T &nodenum,
					   BTreeNode &b)
{

	return ERROR_UNIMPL;
}

ERROR_T BTreeIndex::InsertInternal(const SIZE_T &nodenum,
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
					if (b.info.numkeys < 2*b.info.GetNumSlotsAsLeaf()/3) { //if not 2/3rds full
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
			if (b.info.numkeys < 2*b.info.GetNumSlotsAsLeaf()/3) { //if not 2/3rds full
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

ERROR_T BTreeIndex::Split(const SIZE_T offset,
             const SIZE_T &nodenum,
             BTreeNode &b)
  // SIZE_T &node, SIZE_T &ptr, BTreeNode &b, int offset, SIZE_T split, int &nodeType)
{
  ERROR_T rc;
  SIZE_T newNode;
  BTreeNode parent;
  SIZE_T middle;

  // find middle index of node that needs to be split
  middle = b.info.numkeys / 2;
  // find key at middle index
  KEY_T middleKey;
  rc = b.GetKey(middle, middleKey);
  if (rc) { return rc; }

  // unserialize parent node
  rc = parent.Unserialize(buffercache, b.info.parentnode);
  if (rc) { return rc; }

  // create new node
  rc = AllocateNode(newNode);
  if (rc) { return rc; }
  BTreeNode n(b.info.nodetype, b.info.keysize, b.info.valuesize, buffercache->GetBlockSize());
  n.info.rootnode=b.info.rootnode;
  n.info.parentnode=nodenum;
  n.info.numkeys=0;

  // increment number of keys in parent
  parent.info.numkeys++;

  // find offset in parent
  SIZE_T pOffset;
  SIZE_T tempPtr;
  for (unsigned int i=0; i<=parent.info.numkeys-1; i++)
  {
    // check if the ith ptr points to child node
    if ((unsigned int)parent.GetPtr(i,tempPtr) == nodenum) {
      pOffset=tempPtr;
    }
  }

  // checks if parent needs to be split
  if (parent.info.numkeys > parent.info.GetNumSlotsAsInterior())
  {
    // Split(pOffset,b.info.parentnode,parent);
    return ERROR_UNIMPL;
  }
  // if parent node is not full add key and pointer to new node
  else 
  {
    rc = parent.SetKey(pOffset-1, middleKey);
    if (rc) { return rc; }
    rc = parent.SetPtr(pOffset, newNode);
    if (rc) { return rc; }
  }

  switch(b.info.nodetype)
  {
    case BTREE_LEAF_NODE:
      // copy half of the keys and values to new node
      for (unsigned int i=middle+1; i<=b.info.numkeys; i++)
      {
        // incremember the number of keys in new node
        n.info.numkeys++;
        KEY_T cKey;
        VALUE_T cVal;

        rc = b.GetKey(i, cKey);
        if (rc) { return rc; }
        rc = n.SetKey(i-middle-1, cKey);
        if (rc) { return rc; }
        rc = b.GetVal(i, cVal);
        if (rc) { return rc; }
        rc = n.GetVal(i, cVal);
        if (rc) { return rc; }
      }
      // set new number of keys in child
      b.info.numkeys=middle+1;
      break;
    case BTREE_INTERIOR_NODE:
      // copy half of the keys and pointers to new node
      for (unsigned int i=middle+1; i<=b.info.numkeys; i++)
      {
        // increment the number of keys in new node
        n.info.numkeys++;
        KEY_T cKey;
        SIZE_T cPtr;

        rc = b.GetKey(i, cKey);
        if (rc) { return rc; }
        rc = n.SetKey(i-middle-1, cKey);
        if (rc) { return rc; }
        rc = b.GetPtr(i, cPtr);
        if (rc) { return rc; }
        rc = n.SetPtr(i-middle-1, cPtr);
        if (rc) { return rc; }
      }
      // set new number of keys in child
      b.info.numkeys=middle+1;
      break;
    default:
      return ERROR_INSANE;
  }

  // save changes to disk
  rc = n.Serialize(buffercache, newNode);
  if(rc){return rc;}
  rc = b.Serialize(buffercache, nodenum);
  if(rc){return rc;}
  rc = parent.Serialize(buffercache, b.info.parentnode);
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
  KEY_T prevKey;
  SIZE_T root;

  // get pointer to root node
  root = superblock.info.rootnode;

  return SanityCheckInternal(root, prevKey);
  // return ERROR_UNIMPL;
}

ERROR_T BTreeIndex::SanityCheckInternal(SIZE_T nodenum,
         KEY_T prevKey) const
{
  ERROR_T rc;
  BTreeNode b;
  BTreeNode next;

  rc = b.Unserialize(buffercache, nodenum);
  if (rc) { return rc; }

  // traverse through all pointers/keys
  for (unsigned int i=0; i<b.info.numkeys; i++)
  {
    // check the node type of the pointer
    SIZE_T ptr;
    rc = b.GetPtr(i,ptr);
    if (rc) { return rc; }
    rc = next.Unserialize(buffercache, ptr);

    if (next.info.nodetype!=BTREE_LEAF_NODE)
    {
      rc = SanityCheckInternal(ptr, prevKey);
      if (rc) { return rc; }
    }
    else // the node is a leaf
    {
      // get value at leaf
      KEY_T curKey;
      rc = b.GetKey(i, curKey);
      if (rc) { return rc; }
      // check that if current key is smaller than previous
      if (curKey < prevKey)
      {
        // violation of BTree property
        return ERROR_INSANE;
      }
      else {
        // set new prev key
        prevKey = curKey;
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




