// Μπορείτε να προσθέσετε εδώ βοηθητικές συναρτήσεις για την επεξεργασία Κόμβων Δεδομένων.
#include "bplus_index_node.h"

void print_index_node(indexNode * node)
{
    printf("Pointer counter is %d" , node->pointer_counter);
    for(int i = 0 ; i < 127; i++)
    {
        if(i%2 == 0)
        {
            printf("Pointer is ");
        }
        else{
            printf("Data is ");

        }
        printf("%d\n" , node->pointer_key_array[i]);
    }
}