> For the complete documentation index, see [llms.txt](https://hyperliquid.gitbook.io/hyperliquid-docs/llms.txt). Markdown versions of documentation pages are available by appending `.md` to page URLs; this page is available as [Markdown](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/spot.md).

# Spot

The section documents the info endpoints that are specific to spot.

## Retrieve spot metadata

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

**Headers**

| Name                                           | Value              |
| ---------------------------------------------- | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | "application/json" |

**Body**

| Name                                   | Type   | Description |
| -------------------------------------- | ------ | ----------- |
| type<mark style="color:red;">\*</mark> | String | "spotMeta"  |

**Response**

{% tabs %}
{% tab title="200: OK Successful Response" %}

```json
{
    "tokens": [
        {
            "name": "USDC",
            "szDecimals": 8,
            "weiDecimals" 8,
            "index": 0,
            "tokenId": "0x6d1e7cde53ba9467b783cb7c530ce054",
            "isCanonical": true,
            "evmContract":null,
            "fullName":null
        },
        {
            "name": "PURR",
            "szDecimals": 0,
            "weiDecimals": 5,
            "index": 1,
            "tokenId": "0xc1fb593aeffbeb02f85e0308e9956a90",
            "isCanonical": true,
            "evmContract":null,
            "fullName":null
        },
        {
            "name": "HFUN",
            "szDecimals": 2,
            "weiDecimals": 8,
            "index": 2,
            "tokenId": "0xbaf265ef389da684513d98d68edf4eae",
            "isCanonical": false,
            "evmContract":null,
            "fullName":null
        },
    ],
    "universe": [
        {
            "name": "PURR/USDC",
            "tokens": [1, 0],
            "index": 0,
            "isCanonical": true
        },
        {
            "tokens": [2, 0],
            "name": "@1",
            "index": 1,
            "isCanonical": false
        },
    ]
}
```

{% endtab %}
{% endtabs %}

## Retrieve spot asset contexts

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

#### Headers

| Name                                           | Type   | Description        |
| ---------------------------------------------- | ------ | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | String | "application/json" |

#### Request Body

| Name                                   | Type   | Description            |
| -------------------------------------- | ------ | ---------------------- |
| type<mark style="color:red;">\*</mark> | String | "spotMetaAndAssetCtxs" |

{% tabs %}
{% tab title="200: OK Successful Response" %}

```json
[
{
    "tokens": [
        {
            "name": "USDC",
            "szDecimals": 8,
            "weiDecimals" 8,
            "index": 0,
            "tokenId": "0x6d1e7cde53ba9467b783cb7c530ce054",
            "isCanonical": true,
            "evmContract":null,
            "fullName":null
        },
        {
            "name": "PURR",
            "szDecimals": 0,
            "weiDecimals": 5,
            "index": 1,
            "tokenId": "0xc1fb593aeffbeb02f85e0308e9956a90",
            "isCanonical": true,
            "evmContract":null,
            "fullName":null
        }
    ],
    "universe": [
        {
            "name": "PURR/USDC",
            "tokens": [1, 0],
            "index": 0,
            "isCanonical": true
        }
    ]
},
[
    {
        "dayNtlVlm":"8906.0",
        "markPx":"0.14",
        "midPx":"0.209265",
        "prevDayPx":"0.20432"
    }
]
]
```

{% endtab %}
{% endtabs %}

## Retrieve a user's token balances

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

See a user's token balances.

Under unified account or portfolio margin, this is the source of truth for trading account balance across spot and perps.

#### Headers

| Name                                           | Type | Description        |
| ---------------------------------------------- | ---- | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> |      | "application/json" |

#### Request Body

| Name                                   | Type   | Description                                                                                          |
| -------------------------------------- | ------ | ---------------------------------------------------------------------------------------------------- |
| type<mark style="color:red;">\*</mark> | String | "spotClearinghouseState"                                                                             |
| user<mark style="color:red;">\*</mark> | String | Onchain address in 42-character hexadecimal format; e.g. 0x0000000000000000000000000000000000000000. |

{% tabs %}
{% tab title="200: OK Successful Response" %}

```json
{
    "balances": [
        {
            "coin": "USDC",
            "token": 0,
            "hold": "0.0",
            "total": "14.625485",
            "entryNtl": "0.0"
        },
        {
            "coin": "PURR",
            "token": 1,
            "hold": "0",
            "total": "2000",
            "entryNtl": "1234.56",
        }
    ]
}
```

{% endtab %}
{% endtabs %}

## Retrieve information about the Spot Deploy Auction

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

**Headers**

| Name                                           | Value              |
| ---------------------------------------------- | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | "application/json" |

**Body**

| Name                                   | Type   | Description                                                                                          |
| -------------------------------------- | ------ | ---------------------------------------------------------------------------------------------------- |
| type<mark style="color:red;">\*</mark> | String | "spotDeployState"                                                                                    |
| user<mark style="color:red;">\*</mark> | String | Onchain address in 42-character hexadecimal format; e.g. 0x0000000000000000000000000000000000000000. |

**Response**

{% tabs %}
{% tab title="200: OK Successful Response" %}

```json
{
  "states": [
    {
      "token": 150,
      "spec" : {
        "name": "HYPE",
        "szDecimals": 2,
        "weiDecimals": 8,
      },
      "fullName": "Hyperliquid",
      "spots": [107],
      "maxSupply": 1000000000,
      "hyperliquidityGenesisBalance": "120000",
      "totalGenesisBalanceWei": "100000000000000000",
      "userGenesisBalances": [
        ("0xdddddddddddddddddddddddddddddddddddddddd", "428,062,211")...
      ],
      "existingTokenGenesisBalances": [
        (1, "0")...
      ]
    }
  ],
  "gasAuction": {
    "startTimeSeconds": 1733929200,
    "durationSeconds": 111600,
    "startGas": "181305.90046",
    "currentGas": null,
    "endGas": "181291.247358"
  }
}
```

{% endtab %}
{% endtabs %}

## Retrieve information about the Spot Pair Deploy Auction

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

Note: This returns the status of the Dutch auction for spot pair deployments between existing base and quote tokens. Participation in this auction is permissionless through the same action as the `registerSpot` phase of base token deployment.

#### Headers

| Name                                           | Value              |
| ---------------------------------------------- | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | "application/json" |

#### Body

| Name                                   | Type   | Description                   |
| -------------------------------------- | ------ | ----------------------------- |
| type<mark style="color:red;">\*</mark> | String | "spotPairDeployAuctionStatus" |

{% tabs %}
{% tab title="200: OK Successful Response" %}

```json
{
  "startTimeSeconds":1755468000,
  "durationSeconds":111600,
  "startGas":"500.0",
  "currentGas":"500.0",
  "endGas":null
}
```

{% endtab %}
{% endtabs %}

## Retrieve information about a token

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

**Headers**

| Name                                           | Value              |
| ---------------------------------------------- | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | "application/json" |

**Body**

| Name                                      | Type   | Description                                                                             |
| ----------------------------------------- | ------ | --------------------------------------------------------------------------------------- |
| type<mark style="color:red;">\*</mark>    | String | "tokenDetails"                                                                          |
| tokenId<mark style="color:red;">\*</mark> | String | Onchain id in 34-character hexadecimal format; e.g. 0x00000000000000000000000000000000. |

**Response**

{% tabs %}
{% tab title="200: OK Successful Response" %}

```json
{
  "name": "TEST",
  "maxSupply": "1852229076.12716007",
  "totalSupply": "851681534.05516005",
  "circulatingSupply": "851681534.05516005",
  "szDecimals": 0,
  "weiDecimals": 5,
  "midPx": "3.2049",
  "markPx": "3.2025",
  "prevDayPx": "3.2025",
  "genesis": {
    "userBalances": [
      [
        "0x0000000000000000000000000000000000000001",
        "1000000000.0"
      ],
      [
        "0xffffffffffffffffffffffffffffffffffffffff",
        "1000000000.0"
      ]
    ],
    "existingTokenBalances": []
  },
  "deployer": "0x0000000000000000000000000000000000000001",
  "deployGas": "100.0",
  "deployTime": "2024-06-05T10:50:59.434",
  "seededUsdc": "0.0",
  "nonCirculatingUserBalances": [],
  "futureEmissions": "0.0"
}
```

{% endtab %}
{% endtabs %}

## Retrieve outcome metadata

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

**Headers**

| Name                                           | Value              |
| ---------------------------------------------- | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | "application/json" |

**Body**

| Name                                   | Type   | Description   |
| -------------------------------------- | ------ | ------------- |
| type<mark style="color:red;">\*</mark> | String | "outcomeMeta" |

**Response**

{% tabs %}
{% tab title="200: OK Successful Response" %}

```json
{
  "outcomes": [
    {
      "outcome": 123,
      "name": "Recurring",
      "description": "class:priceBinary|underlying:HYPE|expiry:20260310-1100|targetPrice:34.5|period:3m",
      "sideSpecs": [
        {
          "name": "Yes"
        },
        {
          "name": "No"
        }
      ]
    }
  ]
}
```

{% endtab %}
{% endtabs %}

## Retrieve information about a settled outcome

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

**Headers**

| Name                                           | Value              |
| ---------------------------------------------- | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | "application/json" |

**Body**

| Name                                      | Type   | Description      |
| ----------------------------------------- | ------ | ---------------- |
| type<mark style="color:red;">\*</mark>    | String | "settledOutcome" |
| outcome<mark style="color:red;">\*</mark> | int    | outcome          |

**Response**

{% tabs %}
{% tab title="200: OK Successful Response" %}

```json
{
  "spec": {
    "outcome": 95,
    "name": "Recurring",
    "description": "class:priceBinary|underlying:BTC|expiry:20260526-0600|targetPrice:77363|period:1d",
    "sideSpecs": [
      {
        "name": "Yes"
      },
      {
        "name": "No"
      }
    ],
    "quoteToken": "USDC"
  },
  "settleFraction": "0.0",
  "details": "price:76876.9"
}
```

{% endtab %}
{% endtabs %}

## Retrieve outcome deployer limits

<mark style="color:green;">`POST`</mark> `https://api.hyperliquid.xyz/info`

**Headers**

| Name                                           | Value              |
| ---------------------------------------------- | ------------------ |
| Content-Type<mark style="color:red;">\*</mark> | "application/json" |

**Body**

| Name                                    | Type   | Description             |
| --------------------------------------- | ------ | ----------------------- |
| type<mark style="color:red;">\*</mark>  | String | "outcomeDeployerLimits" |
| venue<mark style="color:red;">\*</mark> | String | venue name              |

**Response**

{% tabs %}
{% tab title="200: OK Successful Response" %}

```json
{
  "nDailyOutcomesRemaining": 50,
  "nActiveOutcomesRemaining": 14
}
```

{% endtab %}
{% endtabs %}
