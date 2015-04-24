package simpledb.buffer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
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
      
      numAvailable = numbuffs;
      for (int i=0; i<numbuffs; i++) {
    	  bufferpool[i] = new Buffer();
      }
         
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      
	   /*for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();*/
      
	   //Flush all allocated buffers. Hence iterate through values in map.
      for(Buffer buff : bufferpoolMap.values()) {
    	  if (buff.isModifiedBy(txnum)) {
    		  System.out.println("DEBUG: Flushing Buffer with block: "+buff.block());
    		  buff.flush();
    	  }
    	        
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
	  
	  System.out.println("DEBUG: BasicBufferManager - pin");
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
         
         //Get currently assigned block, null if unallocated
         Block oldBlock = getExistingBlockForBuffer(buff);
         
         if(oldBlock == null) {
        	 //If unallocated allocate now
        	 bufferpoolMap.put(blk, buff);
         } else {
        	 //Update the map with new mapping
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
	   
	  System.out.println("DEBUG: BasicBufferManager - pinNew");
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      
      //Get currently assigned block, null if unallocated
      Block oldBlock = getExistingBlockForBuffer(buff);
      
      if(oldBlock == null) {
     	 //If buffer unallocated, allocate now
     	 bufferpoolMap.put(buff.block(), buff);
      } else {
     	 //Update the map with new mapping
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
      /*for (Buffer buff : bufferpool) {
         Block b = buff.block();
         if (b != null && b.equals(blk))
            return buff;
      }
      return null;
      */

	   try {
		   System.out.println("DEBUG: FindExistingBuffer value "+bufferpoolMap.get(blk));
	   }catch(NullPointerException e) {
		   System.out.println("DEBUG: FindExistingBuffer value is null"); 
	   }
	   
	   //Return the buffer for the block if mapping exists
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
	   
	   System.out.println("DEBUG:  getExistingBlockForBuffer");
	   Set<Entry<Block, Buffer>> entrySet = bufferpoolMap.entrySet();
	   
	   //Iterate through the Map to get the  block for buffer
		for (Entry<Block, Buffer> entry : entrySet) {
			
			//if buffer matches, return the corresponding block
			if(entry.getValue().equals(buff)) {
				return entry.getKey();
			}
			
		}
	   System.out.println("DEBUG:  Buffer not in Map - Unallocated");
	   return null;
   }
   
   //Update the map by removing the old mapping and inserting new mapping
   private void updateMap(Block oldBlock) {
	   
	   System.out.println("DEBUG:  updateMap");
	   Buffer buffer = bufferpoolMap.remove(oldBlock);
	   Block newBlock = buffer.block();
	   
	   bufferpoolMap.put(newBlock, buffer);   
   }
   
}
