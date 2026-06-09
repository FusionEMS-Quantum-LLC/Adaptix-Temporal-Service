# AdaptixCore pre-push hook

This directory contains the canonical pre-push hook that runs
`scripts/local-ci.sh` before every push.

The hook is enabled by:
  ```
  git config core.hooksPath .githooks
  ```

To bypass (NOT recommended — defeats zero-failure guarantee):
  `git push --no-verify`

To re-install / update the canonical script:
  ```
  bash <(curl -fsSL https://raw.githubusercontent.com/FusionEMS-Quantum-LLC/Adaptix-Governance/main/scripts/install-hook.sh)
  ```

The script itself lives in
[Adaptix-Governance/scripts/local-ci.sh](https://raw.githubusercontent.com/FusionEMS-Quantum-LLC/Adaptix-Governance/main/scripts/local-ci.sh).
