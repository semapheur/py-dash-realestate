window.dashExtensions = Object.assign({}, window.dashExtensions, {
    default: {
        function0: function(feature, context) {
            const {
                classes,
                colorscale,
                style,
                colorProp
            } = context.hideout
            const value = feature.properties[colorProp]

            if (value === null) {
                style.fillColor = colorscale[0]
                return style
            }

            for (let i = 0; i < classes.length; i++) {
                if (value > classes[i]) {
                    style.fillColor = colorscale[i]
                }
            }
            return style
        },
        function1: function(feature, layer, context) {
                layer.bindTooltip(`<b>Total price:</b> ${feature.properties.price_total} NOK</br><b>Ask price:</b> ${feature.properties.price_suggestion} NOK</br><b>Sqm price:</b> ${feature.properties.sqm_price} NOK/m2</br><b>Area:</b> ${feature.properties.area} m2</br><b>Bedrooms:</b> ${feature.properties.bedrooms}`)
            }

            ,
        function2: function(feature, latlng, context) {
            const {
                classes,
                colorscale,
                style,
                colorProp
            } = context.hideout
            const value = feature.properties[colorProp]

            if (value === null) {
                style.fillColor = colorscale[0]
                return L.circleMarker(latlng, style)
            }

            for (let i = 0; i < classes.length; i++) {
                if (value > classes[i]) {
                    style.fillColor = colorscale[i]
                }
            }
            return L.circleMarker(latlng, style)
        }
    }
});