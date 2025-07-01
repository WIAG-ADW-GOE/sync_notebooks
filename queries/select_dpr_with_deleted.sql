SELECT persons.factgrid, persons.id, gsn.nummer, gsn.deleted 
FROM items 
INNER JOIN persons ON persons.item_id = items.id AND items.status = 'online'
INNER JOIN gsn ON gsn.item_id = items.id