import {Protofy} from 'protobase'
import etlApi from "./etl";

const autoApis = Protofy("apis", {
    etl: etlApi
})

export default (app, context) => {
    Object.keys(autoApis).forEach((k) => {
        autoApis[k](app, context)
    })
}