//
// Created by theofilos on 11/4/25.
//

#ifndef BPLUS_BPLUS_FILE_STRUCTS_H
#define BPLUS_BPLUS_FILE_STRUCTS_H
#include "bf.h"
#include "bplus_datanode.h"
#include "bplus_index_node.h"
#include "record.h"
#include "bplus_file_structs.h"

typedef struct {
    TableSchema schema;
    int depth;                           // depth of tree !without! data blocks
    int root_id;                         // the block_id of the root of the Bplus tree
    int record_size;                     // the size of each record in bytes
    int record_capacity_per_block;       // the number of records that fit in a datablock
    int keys_per_block;                  // the number of keys in an indexblock
    int pointers_per_block;              // the number of pointers in an indexblock
} BPlusMeta;

#endif //BPLUS_BPLUS_FILE_STRUCTS_H

