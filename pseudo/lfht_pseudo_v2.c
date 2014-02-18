#define KEY_VALID 0
#define KEY_REMOV 1
#define KEY_INSER 2

#define KEY_BUCKT 4

struct bucket
{
  union
  {		 // snapshot and map point to the same memory location
    uint64_t snapshot;	   //address all 8 bytes of the map with 1 CAS
    struct
    {
      uint32_t version;
      uint8_t  map[KEY_BUCKT];	   
    };
  };
  uint64_t key[KEY_BUCKT];
  void*    val[KEY_BUCKT];
};

/* help functions ************************************************************************* */
int getmap(snapshot, index); /* returns the value of map[index] of snapshot*/
uint64_t setmap(snapshot, index, val); /* returns a new snapshot, where map[index] set to val*/
int getempty(snapshot);	/* returns index i, where map[i] == KEY_REMOV */
uint64_t incversion(snapshot); 	/* return a snapshot with version' = version + 1 */

/* ht operations ************************************************************************** */

/* returns the found element or NULL */
void* search(bucket* b, uint64_t key)
{
  for (int i = 0; i < KEY_BUCKT; i++)
    {
      void* val = b->val[i];
      if (b->key[i] == key && b->map[i] == KEY_VALID) /* disregard any invalid keys */
	{
	  if (b->val[i] == val)
	    return val;
	  else
	    return NULL; 
	}
    }
}


/* returns the removed element or NULL */
void* remove(bucket* b, uint64_t key)
{
  for (int i = 0; i < KEY_BUCKT; i++)
    {
      if (b->key[i] == key && b->map[i] == KEY_VALID) /* disregard any invalid keys */
	{
	  void* removed = b->val[i];
	  if (CAS(&b->key[i], key, NULL)) /* remove key by CASing it out */
	    {
	      b->map[i] = KEY_REMOV;
	      return removed;
	    }
	  else
	    return NULL;
	}
    }

  return NULL;			/* found_index == -1 */
}






/* returns true if inserted, else false */
bool put(bucket* b, uint64_t key, void* val)
{
 retry:
  int i;
  for (i = 0; i < KEY_BUCKT; i++)
    {
      if (b->key[i] == key && b->map[i] == KEY_VALID) /* disregard any invalid keys */
	  return false;
    }

  uint32_t s = b->snapshot;
  int empty = getempty(s);
  uint64_t s1 = setmap(s, empty, KEY_INSER);
  if (CAS(b->snapshot, s, s1) == false)
    goto retry;

  b->key[empty] = key;
  b->val[empty] = val;

  uint64_t s2 = setmap(s, empty, KEY_VALID);
  s2 = incversion(s2);		/* increment the version of the snapshot */
  if (CAS(b->snapshot, s1, s2) == false)
    {
      b->map[empty] = KEY_REMOV; /* see comment (1) below */
      goto retry;
    }

    return true;		/* see comment (2) below */
}


/* (1) this can ofc be optimized by keeping the "empty" spot for the next retry  */
/* (2) the insertion does not handle the case of having no empty key spots in the bucket */
