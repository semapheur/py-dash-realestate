window.dashExtensions = Object.assign({}, window.dashExtensions, {
    default: {
        function0: function(feature, context) {
            const {
                min,
                max,
                colorscale,
                style,
                colorProp
            } = context.hideout
            const csc = chroma.scale(colorscale).domain([min, max])

            const value = feature.properties[colorProp]

            if (value === null) {
                return style
            }

            style.fillColor = csc(value)
            return style
        },
        function1: function(feature, layer, context) {
                if (feature.properties.price_total === undefined) {
                    return
                }

                layer.bindTooltip(`
    <b>Total price:</b> ${feature.properties.price_total} NOK</br>
    <b>Ask price:</b> ${feature.properties.price_suggestion} NOK</br>
    <b>Sqm price:</b> ${feature.properties.sqm_price} NOK/m2</br>
    <b>Area:</b> ${feature.properties.area} m2</br>
    <b>Bedrooms:</b> ${feature.properties.bedrooms}
  `)
            }

            ,
        function2: function(feature, latlng, context) {
            const {
                min,
                max,
                colorscale,
                style,
                colorProp
            } = context.hideout
            const csc = chroma.scale(colorscale).domain([min, max])

            const value = feature.properties[colorProp]

            if (value === undefined) {
                return L.circleMarker(latlng, style)
            }

            style.fillColor = csc(value)
            return L.circleMarker(latlng, style)
        },
        function3: function(feature, latlng, index, context) {
            const {
                min,
                max,
                colorscale,
                style,
                colorProp
            } = context.hideout
            const csc = chroma.scale(colorscale).domain([min, max])

            const leaves = index.getLeaves(feature.properties.cluster_id)
            let valueSum = 0
            for (let i = 0; i < leaves.length; ++i) {
                valueSum += leaves[i].properties[colorProp]
            }
            const valueMean = valueSum / leaves.length

            const scatterIcon = L.DivIcon.extend({
                createIcon: function(oldIcon) {
                    let icon = L.DivIcon.prototype.createIcon.call(this, oldIcon);
                    icon.style.backgroundColor = this.options.color;
                    return icon;
                }
            })
            const icon = new scatterIcon({
                html: '<div style="background-color:white;"><span>' + feature.properties.point_count_abbreviated + '</span></div>',
                className: "marker-cluster",
                iconSize: L.point(40, 40),
                color: csc(valueMean)
            });
            return L.marker(latlng, {
                icon: icon
            })
        }
    }
});