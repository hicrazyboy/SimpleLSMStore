#include "impl.h"
#include <stdint.h>

enum Log_flag{
    e_LogCompBegin = 0,
    e_LogCompEnd = 1,
    e_LogDumpBegin = 2,
    e_LogDumpEnd = 3
};

extern FILE * file_log;

void Logv(FILE * file, const char* format, va_list ap);
void Log(FILE * file, const char* format, ...);
void LogDBState(FILE * file, const Db_state * db_state, const Compaction * c, Log_flag flag);
void LogDBStateSize(FILE * file, const Db_state * db_state);

