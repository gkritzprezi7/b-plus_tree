#ifndef BP_DATANODE_H
#define BP_DATANODE_H
/* Στο αντίστοιχο αρχείο .h μπορείτε να δηλώσετε τις συναρτήσεις
 * και τις δομές δεδομένων που σχετίζονται με τους Κόμβους Δεδομένων.*/
typedef struct bplus_datanode{

    int number_of_records;
    int foo;
    int foobar;
    Record rec_array[5];

} dataNode;



#endif
