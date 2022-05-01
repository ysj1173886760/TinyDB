/**
 * @file type_id.h
 * @author sheep
 * @brief type id
 * @version 0.1
 * @date 2022-05-01
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#ifndef TYPE_ID_H
#define TYPE_ID_H

namespace TinyDB {

// not sure whether i can support such more type
enum TypeId {
    INVALID = 0,
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INTEGER,
    BIGINT,
    DECIMAL,
    VARCHAR,
    TIMESTAMP
};

}

#endif