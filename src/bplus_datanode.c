#include "bplus_datanode.h"
#include "record.h"
#include <stdio.h>

void print_datanode(const TableSchema *schema , dataNode * node)
{
    printf("Number of records is %d\n" , node->number_of_records);
    printf("The next data block is %d\n" , node->next_data_block);
    // iterate through all the records of the dataNode
    for(int i = 0; i < node->number_of_records; i++)
    {
        record_print(schema , &(node->rec_array[i]));
        printf("\n");
    }
}

void insert_in_data_block(dataNode *node, const Record *record, int target)
{
    // we shift all the elements of the record array one position to the right
    // in order to fit the incoming block in correct 'target' position
    for(int i = node->number_of_records - 1 ; i >= target ; i--){
        node->rec_array[i+1] = node->rec_array[i];
    }

    node->rec_array[target] = *record;
    node->number_of_records++;
}
