MATCH (n)
CALL { WITH n
  DETACH DELETE n
} IN TRANSACTIONS OF 10000 ROWS;