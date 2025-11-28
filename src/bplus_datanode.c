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

void insert_in_block(dataNode *node, const Record *record, int target){
    for(int i = node->number_of_records -1 ; i>=target ; i--){
        node->rec_array[i+1] = node->rec_array[i];
    }
    node->rec_array[target] = *record;
    node->number_of_records++;
}
