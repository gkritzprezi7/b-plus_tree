#ifndef BP_INDEX_NODE_H
#define BP_INDEX_NODE_H
/* Στο αντίστοιχο αρχείο .h μπορείτε να δηλώσετε τις συναρτήσεις
 * και τις δομές δεδομένων που σχετίζονται με τους Κόμβους Δεδομένων.*/

void print_index_node(indexNode * node);

typedef struct bplus_index_node{
    
    int pointer_counter;
    int pointer_key_array[127]
    
}indexNode;

#endif