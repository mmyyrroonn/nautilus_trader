> For the complete documentation index, see [llms.txt](https://hyperliquid.gitbook.io/hyperliquid-docs/llms.txt). Markdown versions of documentation pages are available by appending `.md` to page URLs; this page is available as [Markdown](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals.md).

# Perpetuals

The section documents the info endpoints that are specific to perpetuals. See Rate limits section for rate limiting logic and weights.

## Retrieve all perpetual dexs

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

#### Headers

| Name                                           | Type   | Description        |
| ---------------------------------------------- | ------ | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | String | "application/json" |

#### Request Body

| Name                                   | Type   | Description |
| -------------------------------------- | ------ | ----------- |
| type<mark style="color:red;">\*</mark> | String | "perpDexs"  |

{% tabs %}
{% tab title="200: OK Successful Response" %}

```json
[
  null,
  {
    "name": "test",
    "fullName": "test dex",
    "deployer": "0x5e89b26d8d66da9888c835c9bfcc2aa51813e152",
    "oracleUpdater": null,
    "feeRecipient": null,
    "assetToStreamingOiCap": [["COIN1", "100000.0"], ["COIN2", "200000.0"]],
    "assetToFundingMultiplier": [["COIN1", "1.0"], ["COIN2", "2.0"]]
  }
]
```

{% endtab %}
{% endtabs %}

## Retrieve perpetuals metadata (universe and margin tables)

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

#### Headers

| Name                                           | Type   | Description        |
| ---------------------------------------------- | ------ | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | String | "application/json" |

#### Request Body

| Name                                   | Type   | Description                                                                      |
| -------------------------------------- | ------ | -------------------------------------------------------------------------------- |
| type<mark style="color:red;">\*</mark> | String | "meta"                                                                           |
| dex                                    | String | Perp dex name. Defaults to the empty string which represents the first perp dex. |

{% tabs %}
{% tab title="200: OK Successful Response" %}

```json
{
    "universe": [
        {
            "name": "BTC",
            "szDecimals": 5,
            "maxLeverage": 50
        },
        {
            "name": "ETH",
            "szDecimals": 4,
            "maxLeverage": 50
        },
        {
            "name": "HPOS",
            "szDecimals": 0,
            "maxLeverage": 3,
            "onlyIsolated": true
        },
        {
            "name": "LOOM",
            "szDecimals": 1,
            "maxLeverage": 3,
            "isDelisted": true,
            "marginMode": "strictIsolated", // "strictIsolated" means margin cannot be removed, "noCross" means only isolated margin allowed
            "onlyIsolated": true // deprecated. Means either "strictIsolated" or "noCross"
        }
    ],
    "marginTables": [
        [
            50,
            {
                "description": "",
                "marginTiers": [
                    {
                        "lowerBound": "0.0",
                        "maxLeverage": 50
                    }
                ]
            }
        ],
        [
            51,
            {
                "description": "tiered 10x",
                "marginTiers": [
                    {
                        "lowerBound": "0.0",
                        "maxLeverage": 10
                    },
                    {
                        "lowerBound": "3000000.0",
                        "maxLeverage": 5
                    }
                ]
            }
        ]
    ]
}
```

{% endtab %}
{% endtabs %}

## Retrieve perpetuals asset contexts (includes mark price, current funding, open interest, etc.)

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

#### Headers

| Name                                           | Type   | Description        |
| ---------------------------------------------- | ------ | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | String | "application/json" |

#### Request Body

| Name                                   | Type   | Description                                                                      |
| -------------------------------------- | ------ | -------------------------------------------------------------------------------- |
| type<mark style="color:red;">\*</mark> | String | "metaAndAssetCtxs"                                                               |
| dex                                    | String | Perp dex name. Defaults to the empty string which represents the first perp dex. |

{% tabs %}
{% tab title="200: OK Successful Response (first perp dex)" %}

```json
[
{
     "universe": [
        {
            "name": "BTC",
            "szDecimals": 5,
            "maxLeverage": 50
        },
        {
            "name": "ETH",
            "szDecimals": 4,
            "maxLeverage": 50
        },
        {
            "name": "HPOS",
            "szDecimals": 0,
            "maxLeverage": 3,
            "onlyIsolated": true
        }
    ],
    "marginTables":[
         [
            50,
            {
               "description":"",
               "marginTiers":[
                  {
                     "lowerBound":"0.0",
                     "maxLeverage":50
                  }
               ]
            }
         ]
     ],
     "collateralToken":0
},
[
    {
        "dayNtlVlm":"1169046.29406",
         "funding":"0.0000125",
         "impactPxs":[
            "14.3047",
            "14.3444"
         ],
         "markPx":"14.3161",
         "midPx":"14.314",
         "openInterest":"688.11",
         "oraclePx":"14.32",
         "premium":"0.00031774",
         "prevDayPx":"15.322"
    },
    {
         "dayNtlVlm":"1426126.295175",
         "funding":"0.0000125",
         "impactPxs":[
            "6.0386",
            "6.0562"
         ],
         "markPx":"6.0436",
         "midPx":"6.0431",
         "openInterest":"1882.55",
         "oraclePx":"6.0457",
         "premium":"0.00028119",
         "prevDayPx":"6.3611"
      },
      {
         "dayNtlVlm":"809774.565507",
         "funding":"0.0000125",
         "impactPxs":[
            "8.4505",
            "8.4722"
         ],
         "markPx":"8.4542",
         "midPx":"8.4557",
         "openInterest":"2912.05",
         "oraclePx":"8.4585",
         "premium":"0.00033694",
         "prevDayPx":"8.8097"
      }
]
]
```

{% endtab %}

{% tab title="200: OK Successful Response (HIP-3 dex)" %}

```json
[
   {
      "universe":[
         {
            "szDecimals":4,
            "name":"xyz:XYZ100",
            "maxLeverage":20,
            "marginTableId":20,
            "onlyIsolated":true,
            "marginMode":"strictIsolated",
            "growthMode":"enabled",
            "lastGrowthModeChangeTime":"2025-11-23T17:21:40.390706535"
         },
         {
            "szDecimals":3,
            "name":"xyz:TSLA",
            "maxLeverage":10,
            "marginTableId":10,
            "onlyIsolated":true,
            "marginMode":"strictIsolated",
            "growthMode":"enabled",
            "lastGrowthModeChangeTime":"2025-11-23T17:21:40.390706535"
         },
         {
            "szDecimals":3,
            "name":"xyz:NVDA",
            "maxLeverage":10,
            "marginTableId":10,
            "onlyIsolated":true,
            "marginMode":"strictIsolated",
            "growthMode":"enabled",
            "lastGrowthModeChangeTime":"2025-11-23T17:21:40.390706535"
         }
      ],
      "marginTables":[
         [
            50,
            {
               "description":"",
               "marginTiers":[
                  {
                     "lowerBound":"0.0",
                     "maxLeverage":50
                  }
               ]
            }
         ]
      ],
      "collateralToken":0
   },
   [
      {
         "funding":"0.0002110251",
         "openInterest":"0.0854",
         "prevDayPx":"25956.0",
         "dayNtlVlm":"462.9758",
         "premium":"0.0031136686",
         "oraclePx":"25372.0",
         "markPx":"25451.0",
         "midPx":"25451.0",
         "impactPxs":[
            "24946.0",
            "25956.0"
         ],
         "dayBaseVlm":"0.0183"
      },
      {
         "funding":"0.0",
         "openInterest":"12.208",
         "prevDayPx":"447.49",
         "dayNtlVlm":"0.0",
         "premium":null,
         "oraclePx":"450.78",
         "markPx":"465.13",
         "midPx":"464.92",
         "impactPxs":null,
         "dayBaseVlm":"0.0"
      },
      {
         "funding":"0.0",
         "openInterest":"9.43",
         "prevDayPx":"177.0",
         "dayNtlVlm":"2192.853",
         "premium":null,
         "oraclePx":"188.15",
         "markPx":"177.06",
         "midPx":null,
         "impactPxs":null,
         "dayBaseVlm":"12.389"
      }
   ]
]
```

{% endtab %}
{% endtabs %}

## Retrieve user's perpetuals account summary

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

See a user's open positions and margin summary for perpetuals trading.

Under unified account or portfolio margin, use spot balances endpoint instead for trading account balance across spot and perps.

#### Headers

| Name                                           | Type | Description        |
| ---------------------------------------------- | ---- | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> |      | "application/json" |

#### Request Body

| Name                                   | Type   | Description                                                                                          |
| -------------------------------------- | ------ | ---------------------------------------------------------------------------------------------------- |
| type<mark style="color:red;">\*</mark> | String | "clearinghouseState"                                                                                 |
| user<mark style="color:red;">\*</mark> | String | Onchain address in 42-character hexadecimal format; e.g. 0x0000000000000000000000000000000000000000. |
| dex                                    | String | Perp dex name. Defaults to the empty string which represents the first perp dex.                     |

{% tabs %}
{% tab title="200: OK Successful Response" %}

```json
{
  "assetPositions": [
    {
      "position": {
        "coin": "ETH",
        "cumFunding": {
          "allTime": "514.085417",
          "sinceChange": "0.0",
          "sinceOpen": "0.0"
        },
        "entryPx": "2986.3",
        "leverage": {
          "rawUsd": "-95.059824",
          "type": "isolated",
          "value": 20
        },
        "liquidationPx": "2866.26936529",
        "marginUsed": "4.967826",
        "maxLeverage": 50,
        "positionValue": "100.02765",
        "returnOnEquity": "-0.0026789",
        "szi": "0.0335",
        "unrealizedPnl": "-0.0134"
      },
      "type": "oneWay"
    }
  ],
  "crossMaintenanceMarginUsed": "0.0",
  "crossMarginSummary": {
    "accountValue": "13104.514502",
    "totalMarginUsed": "0.0",
    "totalNtlPos": "0.0",
    "totalRawUsd": "13104.514502"
  },
  "marginSummary": {
    "accountValue": "13109.482328",
    "totalMarginUsed": "4.967826",
    "totalNtlPos": "100.02765",
    "totalRawUsd": "13009.454678"
  },
  "time": 1708622398623,
  "withdrawable": "13104.514502"
}
```

{% endtab %}
{% endtabs %}

## Retrieve a user's funding history or non-funding ledger updates

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

Note: Non-funding ledger updates include deposits, transfers, and withdrawals.

#### Headers

| Name                                           | Type   | Description        |
| ---------------------------------------------- | ------ | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | String | "application/json" |

#### Request Body

| Name                                        | Type   | Description                                                                                  |
| ------------------------------------------- | ------ | -------------------------------------------------------------------------------------------- |
| type<mark style="color:red;">\*</mark>      | String | "userFunding" or "userNonFundingLedgerUpdates"                                               |
| user<mark style="color:red;">\*</mark>      | String | Address in 42-character hexadecimal format; e.g. 0x0000000000000000000000000000000000000000. |
| startTime<mark style="color:red;">\*</mark> | int    | Start time in milliseconds, inclusive                                                        |
| endTime                                     | int    | End time in milliseconds, inclusive. Defaults to current time.                               |

{% tabs %}
{% tab title="200: OK Successful Response (first perp dex)" %}

```json
[
    {
        "delta": {
            "coin": "ETH",
            "fundingRate": "0.0000417",
            "szi": "49.1477",
            "type": "funding",
            "usdc":" -3.625312",
            "nSamples": null
        },
        "hash": "0xa166e3fa63c25663024b03f2e0da011a00307e4017465df020210d3d432e7cb8",
        "time": 1681222254710
    },
    ...
]
```

{% endtab %}

{% tab title="200: OK Successful Response (HIP-3 dex)" %}

```json
[
   {
      "delta":{
           "type": "funding",
           "coin": "xyz:XYZ100",
           "usdc": "2.378343",
           "szi": "-15.0",
           "fundingRate": "0.00000625",
           "nSamples": null
      },
      "time": 1767654000068,
      "hash": "0xa166e3fa63c25663024b03f2e0da011a00307e4017465df020210d3d432e7cb9"
   },
   ...
]
```

{% endtab %}
{% endtabs %}

## Retrieve historical funding rates

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

#### Headers

| Name                                           | Type   | Description        |
| ---------------------------------------------- | ------ | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | String | "application/json" |

#### Request Body

| Name                                        | Type   | Description                                                    |
| ------------------------------------------- | ------ | -------------------------------------------------------------- |
| type<mark style="color:red;">\*</mark>      | String | "fundingHistory"                                               |
| coin<mark style="color:red;">\*</mark>      | String | Coin, e.g. "ETH"                                               |
| startTime<mark style="color:red;">\*</mark> | int    | Start time in milliseconds, inclusive                          |
| endTime                                     | int    | End time in milliseconds, inclusive. Defaults to current time. |

{% tabs %}
{% tab title="200: OK (first perp dex)" %}

<pre class="language-json"><code class="lang-json">[
    {
        "coin":"ETH",
        "fundingRate": "-0.00022196",
        "premium": "-0.00052196",
        "time":1683849600076
<strong>    }
</strong>]
</code></pre>

{% endtab %}

{% tab title="200: OK (HIP-3 dex)" %}

```json
[ 
    {
        "coin": "xyz:XYZ100",
        "fundingRate": "-0.00022196",
        "premium": "-0.00052196",
        "time": 1683849600076
    }
]
```

{% endtab %}
{% endtabs %}

## Retrieve predicted funding rates for different venues

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

Note that predicted funding rates is only supported for the first perp dex.

#### Headers

| Name                                           | Type   | Description        |
| ---------------------------------------------- | ------ | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | String | "application/json" |

#### Request Body

| Name                                   | Type   | Description         |
| -------------------------------------- | ------ | ------------------- |
| type<mark style="color:red;">\*</mark> | String | "predictedFundings" |

{% tabs %}
{% tab title="200: OK Successful Response" %}

```json
[
  [
    "AVAX",
    [
      [
        "BinPerp",
        {
          "fundingRate": "0.0001",
          "nextFundingTime": 1733961600000
        }
      ],
      [
        "HlPerp",
        {
          "fundingRate": "0.0000125",
          "nextFundingTime": 1733958000000
        }
      ],
      [
        "BybitPerp",
        {
          "fundingRate": "0.0001",
          "nextFundingTime": 1733961600000
        }
      ]
    ]
  ],...
]
```

{% endtab %}
{% endtabs %}

## Query perps at open interest caps

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

#### Headers

| Name                                           | Type   | Description        |
| ---------------------------------------------- | ------ | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | String | "application/json" |

#### Request Body

| Name                                   | Type   | Description                                                                                     |
| -------------------------------------- | ------ | ----------------------------------------------------------------------------------------------- |
| type<mark style="color:red;">\*</mark> | String | "perpsAtOpenInterestCap"                                                                        |
| dex                                    | String | Perp dex name of builder-deployed dex market. If not specified, then the first perp dex is used |

{% tabs %}
{% tab title="200: OK Successful Response" %}

```json
["BADGER","CANTO","FTM","LOOM","PURR"]
```

{% endtab %}
{% endtabs %}

## Retrieve information about the Perp Deploy Auction

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

#### Headers

| Name                                           | Type   | Description        |
| ---------------------------------------------- | ------ | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | String | "application/json" |

#### Request Body

| Name                                   | Type   | Description               |
| -------------------------------------- | ------ | ------------------------- |
| type<mark style="color:red;">\*</mark> | String | "perpDeployAuctionStatus" |

{% tabs %}
{% tab title="200: OK Successful Response" %}

```json
{
  "startTimeSeconds": 1747656000,
  "durationSeconds": 111600,
  "startGas": "500.0",
  "currentGas": "500.0",
  "endGas": null
}
```

{% endtab %}
{% endtabs %}

## Retrieve User's Active Asset Data

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

#### Headers

| Name                                           | Type   | Description        |
| ---------------------------------------------- | ------ | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | String | "application/json" |

#### Request Body

| Name                                   | Type   | Description                                                                                                                                         |
| -------------------------------------- | ------ | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| type<mark style="color:red;">\*</mark> | String | "activeAssetData"                                                                                                                                   |
| user<mark style="color:red;">\*</mark> | String | Address in 42-character hexadecimal format; e.g. 0x0000000000000000000000000000000000000000.                                                        |
| coin<mark style="color:red;">\*</mark> | String | Coin, e.g. "ETH". See [here](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint#perpetuals-vs-spot) for more details. |

{% tabs %}
{% tab title="200: OK (first perp dex)" %}

```json
{
  "user": "0xb65822a30bbaaa68942d6f4c43d78704faeabbbb",
  "coin": "APT",
  "leverage": {
    "type": "cross",
    "value": 3
  },
  "maxTradeSzs": ["24836370.4400000013", "24836370.4400000013"],
  "availableToTrade": ["37019438.0284740031", "37019438.0284740031"],
  "markPx": "4.4716"
}
```

{% endtab %}

{% tab title="200: OK (HIP-3 dex)" %}

```json
{
   "user": "0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0",
   "coin": "xyz:XYZ100",
   "leverage":{
      "type": "isolated",
      "value": 20,
      "rawUsd": "0.0"
   },
   "maxTradeSzs": [
      "0.0",
      "0.0"
   ],
   "availableToTrade": [
      "0.0",
      "0.0"
   ],
   "markPx": "25451.0"
}
```

{% endtab %}
{% endtabs %}

## Retrieve Builder-Deployed Perp Market Limits

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

#### Headers

| Name                                           | Type   | Description        |
| ---------------------------------------------- | ------ | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | String | "application/json" |

#### Request Body

| Name                                   | Type   | Description                                                                    |
| -------------------------------------- | ------ | ------------------------------------------------------------------------------ |
| type<mark style="color:red;">\*</mark> | String | "perpDexLimits"                                                                |
| dex<mark style="color:red;">\*</mark>  | String | Perp dex name of builder-deployed dex market. The empty string is not allowed. |

{% tabs %}
{% tab title="200: OK" %}

```json
{
  "totalOiCap": "10000000.0",
  "oiSzCapPerPerp": "10000000000.0",
  "maxTransferNtl": "100000000.0",
  "coinToOiCap": [["COIN1", "100000.0"], ["COIN2", "200000.0"]],
}
```

{% endtab %}
{% endtabs %}

## Get Perp Market Status

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

#### Headers

| Name                                           | Type   | Description        |
| ---------------------------------------------- | ------ | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | String | "application/json" |

#### Request Body

| Name                                   | Type   | Description                                                                                   |
| -------------------------------------- | ------ | --------------------------------------------------------------------------------------------- |
| type<mark style="color:red;">\*</mark> | String | "perpDexStatus"                                                                               |
| dex<mark style="color:red;">\*</mark>  | String | Perp dex name of builder-deployed dex market. The empty string represents the first perp dex. |

{% tabs %}
{% tab title="200: OK" %}

```json
{
  "totalNetDeposit": "4103492112.4478230476"
}
```

{% endtab %}
{% endtabs %}

## Retrieve all perpetuals metadata (universe and margin tables)

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

#### Headers

| Name                                           | Type   | Description        |
| ---------------------------------------------- | ------ | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | String | "application/json" |

#### Request Body

| Name                                   | Type   | Description    |
| -------------------------------------- | ------ | -------------- |
| type<mark style="color:red;">\*</mark> | String | "allPerpMetas" |

{% tabs %}
{% tab title="200: OK" %}

```json
[ // first perp dex
    [
        {
            "universe":[
                {
                    "name":"BTC",
                    "szDecimals":5,
                    "maxLeverage":50
                },
                {
                    "name":"ETH",
                    "szDecimals":4,
                    "maxLeverage":50
                },
                {
                    "name":"HPOS",
                    "szDecimals":0,
                    "maxLeverage":3,
                    "onlyIsolated":true
                }
            ],
            "marginTables":[
                [
                    50,
                    {
                        "description":"",
                        "marginTiers":[
                            {
                                "lowerBound":"0.0",
                                "maxLeverage":50
                            }
                        ]
                    }
                ]
            ],
            "collateralToken":0
        },
        [
            {
                "dayNtlVlm":"1169046.29406",
                "funding":"0.0000125",
                "impactPxs":[
                    "14.3047",
                    "14.3444"
                ],
                "markPx":"14.3161",
                "midPx":"14.314",
                "openInterest":"688.11",
                "oraclePx":"14.32",
                "premium":"0.00031774",
                "prevDayPx":"15.322"
            },
            {
                "dayNtlVlm":"1426126.295175",
                "funding":"0.0000125",
                "impactPxs":[
                    "6.0386",
                    "6.0562"
                ],
                "markPx":"6.0436",
                "midPx":"6.0431",
                "openInterest":"1882.55",
                "oraclePx":"6.0457",
                "premium":"0.00028119",
                "prevDayPx":"6.3611"
            },
            {
                "dayNtlVlm":"809774.565507",
                "funding":"0.0000125",
                "impactPxs":[
                    "8.4505",
                    "8.4722"
                ],
                "markPx":"8.4542",
                "midPx":"8.4557",
                "openInterest":"2912.05",
                "oraclePx":"8.4585",
                "premium":"0.00033694",
                "prevDayPx":"8.8097"
            }
        ]
    ],
    [ // second perp dex
        {
            "universe":[
                {
                    "szDecimals":4,
                    "name":"xyz:XYZ100",
                    "maxLeverage":20,
                    "marginTableId":20,
                    "onlyIsolated":true,
                    "marginMode":"strictIsolated",
                    "growthMode":"enabled",
                    "lastGrowthModeChangeTime":"2025-11-23T17:21:40.390706535"
                },
                {
                    "szDecimals":3,
                    "name":"xyz:TSLA",
                    "maxLeverage":10,
                    "marginTableId":10,
                    "onlyIsolated":true,
                    "marginMode":"strictIsolated",
                    "growthMode":"enabled",
                    "lastGrowthModeChangeTime":"2025-11-23T17:21:40.390706535"
                },
                {
                    "szDecimals":3,
                    "name":"xyz:NVDA",
                    "maxLeverage":10,
                    "marginTableId":10,
                    "onlyIsolated":true,
                    "marginMode":"strictIsolated",
                    "growthMode":"enabled",
                    "lastGrowthModeChangeTime":"2025-11-23T17:21:40.390706535"
                }
            ],
            "marginTables":[
                [
                    50,
                    {
                        "description":"",
                        "marginTiers":[
                            {
                                "lowerBound":"0.0",
                                "maxLeverage":50
                            }
                        ]
                    }
                ]
            ],
            "collateralToken":0
        },
        [
            {
                "funding":"0.0002110251",
                "openInterest":"0.0854",
                "prevDayPx":"25956.0",
                "dayNtlVlm":"462.9758",
                "premium":"0.0031136686",
                "oraclePx":"25372.0",
                "markPx":"25451.0",
                "midPx":"25451.0",
                "impactPxs":[
                    "24946.0",
                    "25956.0"
                ],
                "dayBaseVlm":"0.0183"
            },
            {
                "funding":"0.0",
                "openInterest":"12.208",
                "prevDayPx":"447.49",
                "dayNtlVlm":"0.0",
                "premium":null,
                "oraclePx":"450.78",
                "markPx":"465.13",
                "midPx":"464.92",
                "impactPxs":null,
                "dayBaseVlm":"0.0"
            },
            {
                "funding":"0.0",
                "openInterest":"9.43",
                "prevDayPx":"177.0",
                "dayNtlVlm":"2192.853",
                "premium":null,
                "oraclePx":"188.15",
                "markPx":"177.06",
                "midPx":null,
                "impactPxs":null,
                "dayBaseVlm":"12.389"
            }
        ]
    ]
]
```

{% endtab %}
{% endtabs %}

## Retrieve perp annotation

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

#### Headers

| Name                                           | Type   | Description        |
| ---------------------------------------------- | ------ | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | String | "application/json" |

#### Request Body

| Name                                   | Type   | Description           |
| -------------------------------------- | ------ | --------------------- |
| type<mark style="color:red;">\*</mark> | String | "perpAnnotation"      |
| coin<mark style="color:red;">\*</mark> | String | coin name, e.g. "BTC" |

{% tabs %}
{% tab title="200: OK" %}

```json
{
  "category": "other",
  "description": "other perps"
}
```

{% endtab %}
{% endtabs %}

## Retrieve perp categories

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

#### Headers

| Name                                           | Type   | Description        |
| ---------------------------------------------- | ------ | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | String | "application/json" |

#### Request Body

| Name                                   | Type   | Description      |
| -------------------------------------- | ------ | ---------------- |
| type<mark style="color:red;">\*</mark> | String | "perpCategories" |

{% tabs %}
{% tab title="200: OK" %}

```json
[["birb:PENGU","test_cat"],["nq:TEST","preipo"],["nq:TEST1","all"],["nq:TEST2","ai"]]
```

{% endtab %}
{% endtabs %}

## Retrieve concise perp annotations

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

#### Headers

| Name                                           | Type   | Description        |
| ---------------------------------------------- | ------ | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | String | "application/json" |

#### Request Body

| Name                                   | Type   | Description              |
| -------------------------------------- | ------ | ------------------------ |
| type<mark style="color:red;">\*</mark> | String | "perpConciseAnnotations" |

{% tabs %}
{% tab title="200: OK" %}

```json
[    
    [
        "dex:CATS",
        {
            "category": "indices",
            "keywords": ["meow"]
        }
    ],
    [
        "dex:DOGS",
        {
            "category": "indices"
        }
    ]
]
```

{% endtab %}
{% endtabs %}
