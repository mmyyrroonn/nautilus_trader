> For the complete documentation index, see [llms.txt](https://hyperliquid.gitbook.io/hyperliquid-docs/llms.txt). Markdown versions of documentation pages are available by appending `.md` to page URLs; this page is available as [Markdown](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/account-abstraction-modes.md).

# Account abstraction modes

A user's *account abstraction mode* determines how spot and perps balances interact, and whether various assets are used as collateral for perps trading.&#x20;

The supported modes are:

1. Unified account (recommended for most users): single balance for each asset. This balance collateralizes all cross margin positions in that asset and is unified with spot balance in that asset. For example, USDC balance is the single source for validator-operated perps, XYZ perps, and spot trading against USDC as a quote asset. USDT spot balance is the single source for CASH perps and spot trading against USDT as a quote asset.&#x20;
2. Portfolio margin (most capital efficient): single portfolio unifying all eligible assets, which are currently HYPE, BTC, USDC, USDT. See [Portfolio margin](/hyperliquid-docs/trading/portfolio-margin.md) for more details.&#x20;
3. Manual / Standard (recommended for market makers, high volume automated users, and deployers/builders): separate perp and spot balances, separate DEX balances. Cross margin applies to each DEX separately.&#x20;

There is one more mode that was discontinued, included here for completeness:

4. DEX abstraction (discontinued): USDC balances default to perps balance, all other collateral defaults to spot balance. IMPORTANT: Cross margin on HIP-3 DEXs does not behave intuitively for DEX abstraction users.&#x20;

Important details:

1. Builder code addresses must be in standard mode to accrue builder fees
2. Portfolio margin and unified account are limited to 50k user actions per day. Standard mode has no such restrictions.
3. For API users, unified account and portfolio margin show all balances and holds in the spot clearinghouse state. Individual perp dex user states are not meaningful. &#x20;

See Python SDK and [API docs](/hyperliquid-docs/for-developers/api.md) for examples on the agent- and user-signed actions for changing account abstraction modes. &#x20;

### Unified Account Ratio

To compute the precise unified account ratio for monitoring liquidation risk:

```typescript
function computeUnifiedAccountRatio(
    multiverse: Record<string, { index: number; collateralToken: number }>,
    perpDexStates: Array<{
      clearinghouseState: {
        crossMaintenanceMarginUsed: number;
        assetPositions: Array<{
          position: { leverage: { type: string }; marginUsed: number };
        }>;
      };
    }>,
    spotBalances: Array<{ token: number; total: number }>,
  ): number {
    const indexToCollateralToken: Record<number, number> = {};
    for (const meta of Object.values(multiverse)) {
      indexToCollateralToken[meta.index] = meta.collateralToken;
    }

    const crossMarginByToken: Record<number, number> = {};
    const isolatedMarginByToken: Record<number, number> = {};

    for (let index = 0; index < perpDexStates.length; index++) {
      const dex = perpDexStates[index];
      const token = indexToCollateralToken[index];
      if (dex === undefined || token === undefined) continue;

      crossMarginByToken[token] =
        (crossMarginByToken[token] ?? 0) +
        dex.clearinghouseState.crossMaintenanceMarginUsed;

      for (const ap of dex.clearinghouseState.assetPositions) {
        if (ap.position.leverage.type === "isolated") {
          isolatedMarginByToken[token] =
            (isolatedMarginByToken[token] ?? 0) + ap.position.marginUsed;
        }
      }
    }

    let maxRatio = 0;
    for (const [tokenStr, crossMargin] of Object.entries(crossMarginByToken)) {
      const token = Number(tokenStr);
      const spotTotal = spotBalances.find((b) => b.token === token)?.total ?? 0;
      const isolatedMargin = isolatedMarginByToken[token] ?? 0;
      const available = spotTotal - isolatedMargin;
      if (available > 0) {
        maxRatio = Math.max(maxRatio, crossMargin / available);
      }
    }

    return maxRatio;
  }
```
