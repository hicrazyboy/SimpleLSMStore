/************************************************************
  Copyright (C), 2013, ict.ac
  FileName: log.cpp
  Author:        Version :          Date:
  Description:  For print log
  Version:         
  Function List:   
    1. -------
  History:         
      <author>  <time>   <version >   <desc>
      GaoLong    2013/12/19     1.0     build this moudle
***********************************************************/
#include <stdio.h>
//#include <sys/time.h>
#include <time.h>
#include <pthread.h>
#include <string>
#include <assert.h>
#include <stdarg.h>
#include <stdint.h>

#include "log.h"
#include "impl.h"

FILE* file_log;

static uint64_t gettid() {
    pthread_t tid = pthread_self();
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    return thread_id;
}

void Logv(FILE * file, const char* format, va_list ap) {
    const uint64_t thread_id = gettid();

    // We try twice: the first time with a fixed-size stack allocated buffer,
    // and the second time with a much larger dynamically allocated buffer.
    char buffer[500];
    for (int iter = 0; iter < 2; iter++) {
      char* base;
      int bufsize;
      if (iter == 0) {
        bufsize = sizeof(buffer);
        base = buffer;
      } else {
        bufsize = 30000;
        base = new char[bufsize];
      }
      char* p = base;
      char* limit = base + bufsize;

      struct timeval now_tv;
      gettimeofday(&now_tv, NULL);
      const time_t seconds = now_tv.tv_sec;
      struct tm t;
      localtime_r(&seconds, &t);
      p += snprintf(p, limit - p,
                    "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
                    t.tm_year + 1900,
                    t.tm_mon + 1,
                    t.tm_mday,
                    t.tm_hour,
                    t.tm_min,
                    t.tm_sec,
                    static_cast<int>(now_tv.tv_usec),
                    static_cast<long long unsigned int>(thread_id));

      // Print the message
      if (p < limit) {
        va_list backup_ap;
        va_copy(backup_ap, ap);
        p += vsnprintf(p, limit - p, format, backup_ap);
        va_end(backup_ap);
      }

      // Truncate to available space if necessary
      if (p >= limit) {
        if (iter == 0) {
          continue;       // Try again with larger buffer
        } else {
          p = limit - 1;
        }
      }

      // Add newline if necessary
      if (p == base || p[-1] != '\n') {
        *p++ = '\n';
      }

      assert(p <= limit);
      fwrite(base, 1, p - base, file);
      fflush(file);
      if (base != buffer) {
        delete[] base;
      }
      break;
    }
  }


void Log(FILE * file, const char* format, ...) {
    va_list ap;
    va_start(ap, format);
    Logv(file, format, ap);
    va_end(ap);
}

void LogDBState(FILE * file, const Db_state * db_state, const Compaction * c, Log_flag flag) {
    switch (flag) {
        case e_LogCompBegin:
            Log(file, "compacting *******************************************************\n \
Level%d@%d && Level%d@%d to Level%d@unknow sstables\n \
sstables\t%6d %6d %6d %6d %6d %6d %6d \n\
blocks   \t%6d %6d %6d %6d %6d %6d %6d %6d %6d %6d %6d %6d", 
    c->level, c->inputs_[0].size() , c->level+1, c->inputs_[1].size() , c->level+1,
    db_state->db_info->list_of_sst_index_node[0].size(),
    db_state->db_info->list_of_sst_index_node[1].size(),
    db_state->db_info->list_of_sst_index_node[2].size(),
    db_state->db_info->list_of_sst_index_node[3].size(),
    db_state->db_info->list_of_sst_index_node[4].size(),
    db_state->db_info->list_of_sst_index_node[5].size(),
    db_state->db_info->list_of_sst_index_node[6].size(),
    db_state->db_info->list_of_block_index_node[0][0].size(), 
    db_state->db_info->list_of_block_index_node[0][1].size(),
    db_state->db_info->list_of_block_index_node[0][2].size(),
    db_state->db_info->list_of_block_index_node[0][3].size(),
    db_state->db_info->list_of_block_index_node[0][4].size(),
    db_state->db_info->list_of_block_index_node[0][5].size(),
    db_state->db_info->list_of_block_index_node[0][6].size(),
    db_state->db_info->list_of_block_index_node[0][7].size(),
    db_state->db_info->list_of_block_index_node[0][8].size(),
    db_state->db_info->list_of_block_index_node[0][9].size(),
    db_state->db_info->list_of_block_index_node[0][10].size(),
    db_state->db_info->list_of_block_index_node[0][11].size());            
            break;
        case e_LogCompEnd:
            Log(file, "compacted Level%d@%d Level%d@%d to Level%d@%d[%d] sstables[blocks] \n \
sstables\t%6d %6d %6d %6d %6d %6d %6d \n\
blocks   \t%6d %6d %6d %6d %6d %6d %6d %6d %6d %6d %6d %6d \n \
***************************************************************************************************", 
    c->level, c->inputs_[0].size() , c->level+1, c->inputs_[1].size() , c->level+1, 
    c->sst_node_list_outputs.size(),
    c->block_node_list_outputs.size(),
    db_state->db_info->list_of_sst_index_node[0].size(),
    db_state->db_info->list_of_sst_index_node[1].size(),
    db_state->db_info->list_of_sst_index_node[2].size(),
    db_state->db_info->list_of_sst_index_node[3].size(),
    db_state->db_info->list_of_sst_index_node[4].size(),
    db_state->db_info->list_of_sst_index_node[5].size(),
    db_state->db_info->list_of_sst_index_node[6].size(),
    db_state->db_info->list_of_block_index_node[0][0].size(), 
    db_state->db_info->list_of_block_index_node[0][1].size(),
    db_state->db_info->list_of_block_index_node[0][2].size(),
    db_state->db_info->list_of_block_index_node[0][3].size(),
    db_state->db_info->list_of_block_index_node[0][4].size(),
    db_state->db_info->list_of_block_index_node[0][5].size(),
    db_state->db_info->list_of_block_index_node[0][6].size(),
    db_state->db_info->list_of_block_index_node[0][7].size(),
    db_state->db_info->list_of_block_index_node[0][8].size(),
    db_state->db_info->list_of_block_index_node[0][9].size(),
    db_state->db_info->list_of_block_index_node[0][10].size(),
    db_state->db_info->list_of_block_index_node[0][11].size());
            break;
        case e_LogDumpBegin:
            Log(file, "dumping: -------------------------------------------------------\n \
sstables\t%6d %6d %6d %6d %6d %6d %6d \n \
blocks   \t%6d %6d %6d %6d %6d %6d %6d %6d %6d %6d %6d %6d", 
    db_state->db_info->list_of_sst_index_node[0].size(),
    db_state->db_info->list_of_sst_index_node[1].size(),
    db_state->db_info->list_of_sst_index_node[2].size(),
    db_state->db_info->list_of_sst_index_node[3].size(),
    db_state->db_info->list_of_sst_index_node[4].size(),
    db_state->db_info->list_of_sst_index_node[5].size(),
    db_state->db_info->list_of_sst_index_node[6].size(),    
    db_state->db_info->list_of_block_index_node[0][0].size(), 
    db_state->db_info->list_of_block_index_node[0][1].size(),
    db_state->db_info->list_of_block_index_node[0][2].size(),
    db_state->db_info->list_of_block_index_node[0][3].size(),
    db_state->db_info->list_of_block_index_node[0][4].size(),
    db_state->db_info->list_of_block_index_node[0][5].size(),
    db_state->db_info->list_of_block_index_node[0][6].size(),
    db_state->db_info->list_of_block_index_node[0][7].size(),
    db_state->db_info->list_of_block_index_node[0][8].size(),
    db_state->db_info->list_of_block_index_node[0][9].size(),
    db_state->db_info->list_of_block_index_node[0][10].size(),
    db_state->db_info->list_of_block_index_node[0][11].size());
            break;
        case e_LogDumpEnd:
    Log(file, "dumped to Level[%d]: \n \
sstables\t%6d %6d %6d %6d %6d %6d %6d \n \
blocks   \t%6d %6d %6d %6d %6d %6d %6d %6d %6d %6d %6d %6d \n\
---------------------------------------------------------------------------------------------------", 
    c->level_dump,
    db_state->db_info->list_of_sst_index_node[0].size(),
    db_state->db_info->list_of_sst_index_node[1].size(),
    db_state->db_info->list_of_sst_index_node[2].size(),
    db_state->db_info->list_of_sst_index_node[3].size(),
    db_state->db_info->list_of_sst_index_node[4].size(),
    db_state->db_info->list_of_sst_index_node[5].size(),
    db_state->db_info->list_of_sst_index_node[6].size(),    
    db_state->db_info->list_of_block_index_node[0][0].size(), 
    db_state->db_info->list_of_block_index_node[0][1].size(),
    db_state->db_info->list_of_block_index_node[0][2].size(),
    db_state->db_info->list_of_block_index_node[0][3].size(),
    db_state->db_info->list_of_block_index_node[0][4].size(),
    db_state->db_info->list_of_block_index_node[0][5].size(),
    db_state->db_info->list_of_block_index_node[0][6].size(),
    db_state->db_info->list_of_block_index_node[0][7].size(),
    db_state->db_info->list_of_block_index_node[0][8].size(),
    db_state->db_info->list_of_block_index_node[0][9].size(),
    db_state->db_info->list_of_block_index_node[0][10].size(),
    db_state->db_info->list_of_block_index_node[0][11].size());    
            break;
        default:
            break;
    }

}
void LogDBStateSize(FILE * file, const Db_state * db_state) {

    size_t level_size_bytes[MAX_LEVEL] = {0,0,0,0,0,0,0};
    size_t total_size_bytes = {0};
    for (int lvl = 0; lvl < MAX_LEVEL; lvl++) {
        level_size_bytes[lvl] = total_sst_size2(db_state->db_info->list_of_sst_index_node[lvl]);
    }
    Log(file, "level:         0          1          2          3          4          5          6");
    Log(file, "size : %10d %10d %10d %10d %10d %10d %10d",
        level_size_bytes[0],level_size_bytes[1],level_size_bytes[2],level_size_bytes[3],
        level_size_bytes[4],level_size_bytes[5],level_size_bytes[6]);
    Log(file, "Total size = %d", level_size_bytes[0] + level_size_bytes[1] + level_size_bytes[2] + level_size_bytes[3] + level_size_bytes[4] + level_size_bytes[5] + level_size_bytes[6], db_state->db_info->ptr_memtable->curr_memtable_size + (db_state->db_info->ptr_immutable == NULL ? 0 : db_state->db_info->ptr_immutable->curr_memtable_size) );
}

