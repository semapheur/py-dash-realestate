const dagcomponentfuncs = window.dashAgGridComponentFunctions = window.dashAgGridComponentFunctions || {}

/**
 * @param {object} props 
 * @param {string} props.value
 * @param {object} props.data
 * @param {string} props.data.ad_id
 * @returns {React.Element}
 */
dagcomponentfuncs.FinnLink = (props) => {
  const address = props.value
  const finn_id = props.data.ad_id
  console.log(props)
  return React.createElement("a", {
    href: `https://www.finn.no/realestate/homes/ad.html?finnkode=${finn_id}`,
    target: "_blank",
  }, address)
}