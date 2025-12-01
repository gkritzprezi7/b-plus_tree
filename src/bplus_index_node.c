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


void insert_in_index_block(indexNode *node, int key , int pointer){
    
    int target = 2*node->pointer_counter - 1;
    for(int i = 1; i < 2*(node->pointer_counter-1); i += 2){
        if (key < node->pointer_key_array[i]){
            target = i;
            break;
        }
    }
    
    for(int i = 2*(node->pointer_counter - 1) ; i>=target ; i--){
        node->pointer_key_array[i+2] = node->pointer_key_array[i];
    }
    node->pointer_key_array[target] = key;
    node->pointer_key_array[target + 1] = pointer;
    node->pointer_counter++;

    
}
