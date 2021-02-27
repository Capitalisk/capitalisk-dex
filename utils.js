function mapListFields(list, fieldMapper) {
  let fieldList = Object.keys(fieldMapper);
  return list.map((item) => {
    let itemClone = {...item};
    for (let field of fieldList) {
      if (field in itemClone) {
        itemClone[field] = fieldMapper[field](itemClone[field]);
      }
    }
    return itemClone;
  });
}

module.exports = {
  mapListFields
};
