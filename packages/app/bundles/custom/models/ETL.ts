export class ETL {
    entityModel: string;
    raw_data: any[];

    constructor(entityModel: string, raw_data: any[]) {
        this.entityModel = entityModel;
        this.raw_data = raw_data;
    }

    transform() {
        switch (this.entityModel) {
            case "promotions":
                // FIX: Promotions NFQ Data problems
                const data = this.raw_data?.reduce((total, item) => {
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
                return data
                break;
            default:
                return this.raw_data;
                break;
        }
    }
}