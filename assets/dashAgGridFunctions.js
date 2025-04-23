var dagfuncs = window.dashAgGridFunctions = window.dashAgGridFunctions || {}

dagfuncs.DateFormatter = function (dateInt) {
  const date = new Date(dateInt * 1000)
  return date.toLocaleDateString('en-GB', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  })
}