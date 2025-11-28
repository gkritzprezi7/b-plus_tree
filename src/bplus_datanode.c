#include "bplus_datanode.h"
#include "record.h"

void print_datanode(const TableSchema *schema , dataNode * node)
{
    printf("Number of records is %d\n" , node->number_of_records);

    for(int i = 0; i < node->number_of_records; i++)
    {
        record_print(schema , node);
    }
}