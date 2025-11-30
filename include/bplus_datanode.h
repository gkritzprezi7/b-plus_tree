#ifndef BP_DATANODE_H
#define BP_DATANODE_H
/* Στο αντίστοιχο αρχείο .h μπορείτε να δηλώσετε τις συναρτήσεις
 * και τις δομές δεδομένων που σχετίζονται με τους Κόμβους Δεδομένων.*/

void print_datanode(const TableSchema *schema , dataNode * node);
void insert_in_block(dataNode *node, const Record *record, int target);

typedef struct bplus_datanode{

    int number_of_records;
    int next_data_block;
    int foobar;
    Record rec_array[5];

} dataNode;



#endif
