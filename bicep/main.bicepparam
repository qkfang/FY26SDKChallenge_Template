// main.bicepparam
// Default parameter values for main.bicep.
// Override these with a custom .bicepparam file or --parameters flags.

using './main.bicep'

param location = 'australiaeast'
param capacityName = 'fabriccapacitycicd'
param capacitySkuName = 'F2'
param capacityAdminMembers = [
  'a6efe236-83c5-472b-a068-65006e369ad7' // sp-demo-01,
  'danielfang@MngEnvMCAP951655.onmicrosoft.com' // mine
]


