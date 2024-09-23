export class ETL {
    entityModel: string;
    raw_data: any[];

    constructor(entityModel: string, raw_data: any[]) {
        this.entityModel = entityModel;
        this.raw_data = raw_data;
    }

    transform() {
        let data;
        switch (this.entityModel) {
            case "promotions":
                // FIX: Promotions NFQ Data problems
                data = this.raw_data?.reduce((total, item) => {
                    const itemFoundIndex = total.findIndex((i) => i.id.S === item.id.S); // -1 when not found
                    if (item.hasOwnProperty('id')) {
                        if (itemFoundIndex == -1) { // Add item bc is new item
                            return total.concat(item);
                        }
                        else { // Modify products with existing items and incoming item
                            total[itemFoundIndex] = { ...total[itemFoundIndex], products: { SS: total[itemFoundIndex].products.SS.concat(item.products.SS) } }
                            return total;
                        }
                    }
                    return total
                }, []);
                return data;
            case "checklists":
                const sanitize = (_data) => {
                    const tmpRawData = { ..._data }
                    Object.keys(_data).forEach((key) => {
                        const keyValue = tmpRawData[key];
                        if(key == "status" && keyValue.L == "") {
                            tmpRawData["status"].L = [];
                        }
                    });
                    return tmpRawData;
                }
                data = this.raw_data.map((item) => sanitize(item))
                return data;
            default:
                return this.raw_data;
        }
    }
}