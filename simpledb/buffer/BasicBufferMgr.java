package simpledb.buffer;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import simpledb.file.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;	//A pool of Unallocated buffers
   
   //Map of allocated buffers, keyed on the blocks they contain
   //This maps a block to a buffer. Contains only the allocated buffers
   private Map<Block,Buffer> bufferpoolMap;
   private int numAvailable;
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
      bufferpool = new Buffer[numbuffs];
      
      //Initialize the Map
      bufferpoolMap = new HashMap<Block,Buffer>();
      
      //Initialize the array of buffers
      numAvailable = numbuffs;
      for (int i=0; i<numbuffs; i++)
    	  bufferpool[i] = new Buffer();
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
	   //Flush buffers modified by a particular transaction
	   //Iterate through values in map.
	   for(Buffer buff : bufferpoolMap.values()) {
		   if (buff.isModifiedBy(txnum))
			   buff.flush();
      }
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
         
         //Get the block that is currently assigned to buff, null if unallocated
         Block oldBlock = getExistingBlockForBuffer(buff);
         
         if(oldBlock == null) {
        	 //If buffer is unallocated, add a mapping
        	 bufferpoolMap.put(blk, buff);
         } else {
        	 //If buffer is already allocated, Update the mapping
        	 updateMap(oldBlock);
         }
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      return buff;
   }
   

/**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      
      //Get the block that is currently assigned to buff, null if unallocated
      Block oldBlock = getExistingBlockForBuffer(buff);
      
      if(oldBlock == null) {
     	 //If buffer is unallocated, add a mapping
     	 bufferpoolMap.put(buff.block(), buff);
      } else {
     	 //If buffer is already allocated, Update the mapping
     	 updateMap(oldBlock);
      }
      numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned())
         numAvailable++;
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   private Buffer findExistingBuffer(Block blk) {
	   //Return the buffer for the block, if mapping exists
	   //Else return null
	   return bufferpoolMap.get(blk);
   }
   
   private Buffer chooseUnpinnedBuffer() {
      for (Buffer buff : bufferpool)
         if (!buff.isPinned())
         return buff;
      return null;
   }
   
   private Block getExistingBlockForBuffer(Buffer buff) {
	   //Store all the key value pairs from the mapping as a set
	   Set<Entry<Block, Buffer>> entrySet = bufferpoolMap.entrySet();
	   
	   //Iterate through the Map to get the  block for buffer
		for (Entry<Block, Buffer> entry : entrySet) {
			//if buffer matches, return the corresponding block
			if(entry.getValue().equals(buff))
				return entry.getKey();
		}
	   //If the buffer is not allocated and does not contain any block, return null
	   return null;
   }
   
   //Update the map by changing the key from old block to new block
   private void updateMap(Block oldBlock) {
	   //Remove the block - buffer mapping for old block
	   Buffer buffer = bufferpoolMap.remove(oldBlock);
	   
	   //Get the new block pinned to the buffer
	   Block newBlock = buffer.block();
	   
	   //Add the new block - buffer mapping
	   bufferpoolMap.put(newBlock, buffer);   
   }
}
