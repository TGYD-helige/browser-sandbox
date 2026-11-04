# Image release

The runtime remains a development preview until release validation and the applicable upstream authorizations are complete. Building an image locally, distributing that image, and offering browser control to third parties are different uses. A successful build does not establish permission for either of the latter uses.

## Upstream terms and evidence

The pinned Browserless 2.57.0 source is offered under SSPL-1.0 or a commercial license. Review the [versioned license](https://github.com/browserless/browserless/blob/v2.57.0/LICENSE) and the applicable agreement for the intended distribution and service. The repository's MIT license covers its own source only.

CloakBrowser's [binary license](https://github.com/CloakHQ/CloakBrowser/blob/main/BINARY-LICENSE.md), reviewed on 2026-11-04, distinguishes its MIT wrapper from the compiled browser. The public binary terms allow internal infrastructure use under their stated conditions, restrict redistribution, and require separate authorization for customer-controlled browser capabilities. An ordinary download or subscription is not evidence of distribution or hosted-service rights. Verify the terms applicable to the exact pinned binary and any separate agreement before shipping it.

The project contains no verified grant permitting public redistribution of the combined image or customer-controlled hosted browser access. Maintainers must retain the relevant agreements and the authorized distribution scope in their own records; keep contract details and credentials out of public repository files and logs. Set repository variables `BROWSERLESS_IMAGE_DISTRIBUTION=approved` and `CLOAKBROWSER_IMAGE_DISTRIBUTION=approved` only after the responsible owner has verified image distribution rights for the configured registry and audience. These variables record an owner's decision; the workflow cannot verify a legal grant. Review hosted-service authorization separately before deployment.

## Publish a verified image

Merge the release source through the protected master PR flow. Wait for Runtime CI and Browser integration to succeed at the exact master commit, including both architectures, consumer checks, and performance reports. Update the package version for each new release; version tags are never reused.

Run **Release browser image** manually on `master`, with the matching `v<package.json version>` tag. The workflow checks the recorded distribution decisions and successful CI, builds and verifies each architecture on a native runner, and pushes architecture images to `ghcr.io/<owner>/browser-sandbox`. It then creates the versioned AMD64/ARM64 manifest and a draft GitHub release containing the manifest record. It does not deploy a service or publish an npm package. Registry visibility and its permitted audience must match the recorded authorization before dispatch.

Inspect the draft, manifest digest, CI reports, and registry access before publishing the GitHub release. Consumers should pin the manifest digest. If one architecture fails, the versioned manifest is not created; successful intermediate build tags may remain in the registry. If a later release step fails, inspect the existing manifest and draft before retrying; the workflow refuses to overwrite an existing version tag. Remove obsolete intermediate build tags under the registry's normal retention policy.
