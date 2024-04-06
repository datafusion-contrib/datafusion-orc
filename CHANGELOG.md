

## v0.3.0 (2024-04-06)

### Chore

 - <csr-id-c928625450a3dad6d5d06e50a64e3c7a1d7afb0d/> bump arrow to 48.0
 - <csr-id-b78e9da99bd7ecfcfefd3d1d5cd66736990d658e/> apply suggestions from CR
 - <csr-id-501a8a96e01ec563ab1d6dd8c4358bf277e5e641/> add codecov config
 - <csr-id-feb932c0ea21a79d58f6ba61749637839e579e2a/> apply suggestions from CR
 - <csr-id-995e0f34aa6e6a89c1c5f10fecf124344db3e753/> apply suggestions from CR
 - <csr-id-40b2a3ec2bbb867ffbd6b7e800db7ac14839ddfe/> remove unstable features
 - <csr-id-aa29e38147616b18ee20a8d379e3bb8f8cf17f53/> add comments for tracking the provenance
 - <csr-id-fc2a9c374c74492a8fa8e44cf6fc6afbb1fcc5e9/> update Cargo.toml
 - <csr-id-fa406969df9868ae2bf2371ad549a519ae12f7ab/> remove MIT LICENSE
 - <csr-id-b3d04a404504584c0de75fc5c18afd2515a3d5e7/> bump 0.2.43
 - <csr-id-0348b466a47c6701fef107e78c353eeeedea59d7/> bump arrow to 47.0
   * chore: bump arrow to 47.0
 - <csr-id-4fd48d8b8d5f817198b812bb88e6fa4ce209d74f/> bump 0.2.42
 - <csr-id-3cb3b7ed26ed3059c3c7e3854553bf58d58fd726/> bump 0.2.42
 - <csr-id-dcb7005a7a27648d97c1ce4a36cae2864e73ff7c/> fmt toml
 - <csr-id-03cfc053c33dcb481a5d1bf9fc9b8af56b0a73b7/> bump version to 0.2.41
 - <csr-id-5596dc34b47d6b9f000a327cf659af5138745f8e/> bump arrow to 43.0
 - <csr-id-a8cf57a0b8f71ae3a95770a0519245ae698f1a59/> bump version to 0.2.4
 - <csr-id-5840708dfb65a12b1568cc266a94105c488becb0/> bump version to 0.2.3
 - <csr-id-628680c6ca6ad7f05c6cf3e4ac89aef0ce52acb7/> bump version to 0.2.2
 - <csr-id-ac249e4a30c8d261dbf6351950c3794277842ced/> fmt toml
 - <csr-id-90d2debeda6b7625f36a5c411e065f0c76d45ccb/> make create_arrow_schema return Schema
 - <csr-id-74ea42fd8879c5c5ca580271f282f4816aa15f54/> downgrade arrow version
 - <csr-id-3c9c62e6aab505dd666e63dc5e6f892d7c7f04b2/> bump version to 0.2.0
 - <csr-id-223147575cca59d4afa5eb0531be60cdd5ec373b/> udpate Cargo.toml
 - <csr-id-616fac90b0027909328cfeec36da0a52f07b3b9e/> add test data
 - <csr-id-47572917cf51a8198733645b071fded8f59ff5c4/> add licenses
 - <csr-id-c9b269937f24bb33eb03391fa174024db69bd10c/> add ci cfg

### Documentation

 - <csr-id-218d99165fb2190764ac4345a3a13f9dbaae5135/> update CI badger in README.md

### New Features

 - <csr-id-424b02140b2d3cfa93afa7479cb1f6a29dbd9365/> support to map datatype
 - <csr-id-6a7267e4f8080958faa6ebba7bc9f3b9f0e321f2/> support to list datatype
 - <csr-id-b4828e49705b21216d4828c3d4c1dd446cdd88cd/> support to struct datatype
 - <csr-id-3378874ee7b60491c184254cad613eddd630fa5c/> map tinyint to i8
 - <csr-id-cf13577180e414c96a77588554231d0842361768/> support to read tinyint
 - <csr-id-cbc63710a4fac1ef8f89a4905b11fb113c6f26f0/> Support all decompression types
   * Support all decompression types
   
   * Move default compression block size to const
 - <csr-id-0a98496ddf040660446398ac1512a4f8eb837b97/> support binary decoding
   * feat: support binary data type
   
   * fix: fmt
   
   * chore: add Cargo.lock
   
   * chore: fix clippy
   
   ---------
 - <csr-id-a2c8ae529ef252129784d28af70e5b36f27172fd/> support to zstd compression
 - <csr-id-875a08d9258e2e1eb6e6eed1c0426e8739c7aa27/> make ArrowStreamReader sendable
 - <csr-id-ce65c5881ce8765d1a9cd484b980b8d98ca4b86b/> add schema method for ArrowStreamReader
 - <csr-id-58af78886b26673ce523f201537c7dac78b5a3d7/> add async arrow reader
 - <csr-id-3ed1163853975d3a722b9b54717ceea9d0eb7fce/> init implementation

### Other

 - <csr-id-ccba51184f380d4bb478e4f9fb980cb6e8cffa4d/> update README.md
 - <csr-id-9edb6095705d4d590e67961ee0068ffd458aa5fa/> update README.md
 - <csr-id-da265dbb8f74913227961b8a127e78521b97df32/> update README.md
 - <csr-id-8b99d475825c7233496d9f3d36aa997101dc3ab6/> add README.md

### Refactor

 - <csr-id-d438c1dd855f2f553507cd3cafcb7fe559298186/> refactor byte/boolean iter
 - <csr-id-e037b1406755712470731edc439496fe6fe37a90/> remove StripeFactoryInner
 - <csr-id-d5b1542dc4093264d308be1675803e363bcccafe/> move arrow to arrow reader
 - <csr-id-c43a8c5efc5c4fdc13bf7f3d818fc42b6518d525/> rle v2 reader

### Test

 - <csr-id-6a342245b9cac221ab7ab644c64f6dfa99f4fe56/> add pathed base rle v2 test
 - <csr-id-a0704a4dac680a18e049b3855a9dd38f5b12a04e/> test Cursor::root
 - <csr-id-2275cb2e53192fe11afe93d5e9971c3b5ad60c8b/> add tests

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 150 commits contributed to the release over the course of 155 calendar days.
 - 51 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 33 unique issues were worked on: [#10](https://github.com/datafusion-contrib/datafusion-orc/issues/10), [#16](https://github.com/datafusion-contrib/datafusion-orc/issues/16), [#20](https://github.com/datafusion-contrib/datafusion-orc/issues/20), [#23](https://github.com/datafusion-contrib/datafusion-orc/issues/23), [#24](https://github.com/datafusion-contrib/datafusion-orc/issues/24), [#3](https://github.com/datafusion-contrib/datafusion-orc/issues/3), [#31](https://github.com/datafusion-contrib/datafusion-orc/issues/31), [#35](https://github.com/datafusion-contrib/datafusion-orc/issues/35), [#36](https://github.com/datafusion-contrib/datafusion-orc/issues/36), [#38](https://github.com/datafusion-contrib/datafusion-orc/issues/38), [#39](https://github.com/datafusion-contrib/datafusion-orc/issues/39), [#4](https://github.com/datafusion-contrib/datafusion-orc/issues/4), [#40](https://github.com/datafusion-contrib/datafusion-orc/issues/40), [#43](https://github.com/datafusion-contrib/datafusion-orc/issues/43), [#44](https://github.com/datafusion-contrib/datafusion-orc/issues/44), [#45](https://github.com/datafusion-contrib/datafusion-orc/issues/45), [#48](https://github.com/datafusion-contrib/datafusion-orc/issues/48), [#49](https://github.com/datafusion-contrib/datafusion-orc/issues/49), [#5](https://github.com/datafusion-contrib/datafusion-orc/issues/5), [#50](https://github.com/datafusion-contrib/datafusion-orc/issues/50), [#51](https://github.com/datafusion-contrib/datafusion-orc/issues/51), [#53](https://github.com/datafusion-contrib/datafusion-orc/issues/53), [#54](https://github.com/datafusion-contrib/datafusion-orc/issues/54), [#57](https://github.com/datafusion-contrib/datafusion-orc/issues/57), [#6](https://github.com/datafusion-contrib/datafusion-orc/issues/6), [#60](https://github.com/datafusion-contrib/datafusion-orc/issues/60), [#64](https://github.com/datafusion-contrib/datafusion-orc/issues/64), [#65](https://github.com/datafusion-contrib/datafusion-orc/issues/65), [#69](https://github.com/datafusion-contrib/datafusion-orc/issues/69), [#7](https://github.com/datafusion-contrib/datafusion-orc/issues/7), [#73](https://github.com/datafusion-contrib/datafusion-orc/issues/73), [#8](https://github.com/datafusion-contrib/datafusion-orc/issues/8), [#9](https://github.com/datafusion-contrib/datafusion-orc/issues/9)

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **[#10](https://github.com/datafusion-contrib/datafusion-orc/issues/10)**
    - Bump 0.2.43 ([`b3d04a4`](https://github.com/datafusion-contrib/datafusion-orc/commit/b3d04a404504584c0de75fc5c18afd2515a3d5e7))
 * **[#16](https://github.com/datafusion-contrib/datafusion-orc/issues/16)**
    - Add timestamp instant to README ([`abd9960`](https://github.com/datafusion-contrib/datafusion-orc/commit/abd9960d3f4a6325ecdc5f732fa55c6468784b1b))
 * **[#20](https://github.com/datafusion-contrib/datafusion-orc/issues/20)**
    - Support all decompression types ([`cbc6371`](https://github.com/datafusion-contrib/datafusion-orc/commit/cbc63710a4fac1ef8f89a4905b11fb113c6f26f0))
 * **[#23](https://github.com/datafusion-contrib/datafusion-orc/issues/23)**
    - Refactor varint handling ([`9f6c216`](https://github.com/datafusion-contrib/datafusion-orc/commit/9f6c2164b380dc4e954320b9adc4f408eb305baa))
 * **[#24](https://github.com/datafusion-contrib/datafusion-orc/issues/24)**
    - Support for Int RLE v1 encoding ([`2b7a2b0`](https://github.com/datafusion-contrib/datafusion-orc/commit/2b7a2b01c0da17ddcb0731600d4f30440cd12350))
 * **[#3](https://github.com/datafusion-contrib/datafusion-orc/issues/3)**
    - Crate for generating proto.rs from orc_proto.proto ([`47bb450`](https://github.com/datafusion-contrib/datafusion-orc/commit/47bb450f3aa14f0cbcdcf0b6db6c86c76cdf5e4d))
 * **[#31](https://github.com/datafusion-contrib/datafusion-orc/issues/31)**
    - Add codecov config ([`501a8a9`](https://github.com/datafusion-contrib/datafusion-orc/commit/501a8a96e01ec563ab1d6dd8c4358bf277e5e641))
 * **[#35](https://github.com/datafusion-contrib/datafusion-orc/issues/35)**
    - Update README.md ([`8f5ed40`](https://github.com/datafusion-contrib/datafusion-orc/commit/8f5ed4018662322b4460929405dc7b8fb79aea95))
 * **[#36](https://github.com/datafusion-contrib/datafusion-orc/issues/36)**
    - Refactor stream retrieval and datatype iterators ([`a12cae7`](https://github.com/datafusion-contrib/datafusion-orc/commit/a12cae7d11f7921fab9a0f2d208b313aac4722f1))
 * **[#38](https://github.com/datafusion-contrib/datafusion-orc/issues/38)**
    - Remove Chrono dependency ([`f19cc7b`](https://github.com/datafusion-contrib/datafusion-orc/commit/f19cc7b66762791aba00a32a20615cb5466b33ed))
 * **[#39](https://github.com/datafusion-contrib/datafusion-orc/issues/39)**
    - Add initial simple benchmark ([`cbeb8bd`](https://github.com/datafusion-contrib/datafusion-orc/commit/cbeb8bdb3e90bc1d8c1d8a12df9d07baec905617))
 * **[#4](https://github.com/datafusion-contrib/datafusion-orc/issues/4)**
    - Remove unstable features ([`40b2a3e`](https://github.com/datafusion-contrib/datafusion-orc/commit/40b2a3ec2bbb867ffbd6b7e800db7ac14839ddfe))
    - Support binary decoding ([`0a98496`](https://github.com/datafusion-contrib/datafusion-orc/commit/0a98496ddf040660446398ac1512a4f8eb837b97))
 * **[#40](https://github.com/datafusion-contrib/datafusion-orc/issues/40)**
    - Remove Cargo.lock, add to .gitignore ([`01eee6e`](https://github.com/datafusion-contrib/datafusion-orc/commit/01eee6e4904542799540e348887639a675cc211f))
 * **[#43](https://github.com/datafusion-contrib/datafusion-orc/issues/43)**
    - Refactor synchronous parsing of file tail metadata ([`eed416c`](https://github.com/datafusion-contrib/datafusion-orc/commit/eed416c64d71e23c224444a6b524c7d8cc0fed60))
 * **[#44](https://github.com/datafusion-contrib/datafusion-orc/issues/44)**
    - Refactor to decouple from relying directly on proto ([`cae1014`](https://github.com/datafusion-contrib/datafusion-orc/commit/cae10149f4b2234c632e1ba67776c6ff6b6289b6))
 * **[#45](https://github.com/datafusion-contrib/datafusion-orc/issues/45)**
    - Refactor schema/type handling ([`a86dda2`](https://github.com/datafusion-contrib/datafusion-orc/commit/a86dda255478eb663fcaed762a5cdcc081862b9b))
 * **[#48](https://github.com/datafusion-contrib/datafusion-orc/issues/48)**
    - Remove Reader struct, condense into Cursor ([`3e48a6e`](https://github.com/datafusion-contrib/datafusion-orc/commit/3e48a6efcf6552990b2aab5dbc8585d5739237b8))
 * **[#49](https://github.com/datafusion-contrib/datafusion-orc/issues/49)**
    - Bump arrow to 48.0 ([`c928625`](https://github.com/datafusion-contrib/datafusion-orc/commit/c928625450a3dad6d5d06e50a64e3c7a1d7afb0d))
 * **[#5](https://github.com/datafusion-contrib/datafusion-orc/issues/5)**
    - Fmt toml ([`dcb7005`](https://github.com/datafusion-contrib/datafusion-orc/commit/dcb7005a7a27648d97c1ce4a36cae2864e73ff7c))
 * **[#50](https://github.com/datafusion-contrib/datafusion-orc/issues/50)**
    - Refactor Integer RLE V2 handling ([`f5a5b21`](https://github.com/datafusion-contrib/datafusion-orc/commit/f5a5b21148d2787ff1cd34996b7cf6dadfd643e9))
 * **[#51](https://github.com/datafusion-contrib/datafusion-orc/issues/51)**
    - Introduce ProjectionMask ([`3863577`](https://github.com/datafusion-contrib/datafusion-orc/commit/3863577ae31084cc7afbd72d846d16a076b88832))
 * **[#53](https://github.com/datafusion-contrib/datafusion-orc/issues/53)**
    - Add ArrowReaderBuilder ([`ebe96e0`](https://github.com/datafusion-contrib/datafusion-orc/commit/ebe96e070eafcb797cb33cb02aa2c935767a4825))
 * **[#54](https://github.com/datafusion-contrib/datafusion-orc/issues/54)**
    - Switch CI toolchain to stable ([`2cb322a`](https://github.com/datafusion-contrib/datafusion-orc/commit/2cb322a1479703f10526e9089c2bf45622c250e1))
 * **[#57](https://github.com/datafusion-contrib/datafusion-orc/issues/57)**
    - Introduce generics into RLE Int decoders ([`35ab125`](https://github.com/datafusion-contrib/datafusion-orc/commit/35ab1250b8cf93c4a738240fecb4843015ef558c))
 * **[#6](https://github.com/datafusion-contrib/datafusion-orc/issues/6)**
    - Update CI badger in README.md ([`218d991`](https://github.com/datafusion-contrib/datafusion-orc/commit/218d99165fb2190764ac4345a3a13f9dbaae5135))
    - Bump 0.2.42 ([`4fd48d8`](https://github.com/datafusion-contrib/datafusion-orc/commit/4fd48d8b8d5f817198b812bb88e6fa4ce209d74f))
    - Bump 0.2.42 ([`3cb3b7e`](https://github.com/datafusion-contrib/datafusion-orc/commit/3cb3b7ed26ed3059c3c7e3854553bf58d58fd726))
 * **[#60](https://github.com/datafusion-contrib/datafusion-orc/issues/60)**
    - Mark ArrowReader as Send ([`6fdab55`](https://github.com/datafusion-contrib/datafusion-orc/commit/6fdab5505227f7b13ee214562a0001a5e8e2ef36))
 * **[#64](https://github.com/datafusion-contrib/datafusion-orc/issues/64)**
    - Put async functionality behind feature ([`73234be`](https://github.com/datafusion-contrib/datafusion-orc/commit/73234be74216f7e76d1ac316580c66ed8104a191))
 * **[#65](https://github.com/datafusion-contrib/datafusion-orc/issues/65)**
    - Add integration tests using example files from apache/orc ([`8e254a6`](https://github.com/datafusion-contrib/datafusion-orc/commit/8e254a68ff062907da98bc094b7511b24d165f55))
 * **[#69](https://github.com/datafusion-contrib/datafusion-orc/issues/69)**
    - Run examples as part of CI ([`e76c5cd`](https://github.com/datafusion-contrib/datafusion-orc/commit/e76c5cd70b195f0e9af67aff96c0193416217d90))
 * **[#7](https://github.com/datafusion-contrib/datafusion-orc/issues/7)**
    - Update README.md ([`435a52e`](https://github.com/datafusion-contrib/datafusion-orc/commit/435a52ee7442d7793cdd8838d731f496077dc47a))
 * **[#73](https://github.com/datafusion-contrib/datafusion-orc/issues/73)**
    - Generate expected data for integration tests as feather files ([`fd23fdb`](https://github.com/datafusion-contrib/datafusion-orc/commit/fd23fdb61599dd52753d70fc808babd289e5c422))
 * **[#8](https://github.com/datafusion-contrib/datafusion-orc/issues/8)**
    - Update README.md ([`865d43b`](https://github.com/datafusion-contrib/datafusion-orc/commit/865d43b9226096753776017a2d0bfc2e14db01f4))
 * **[#9](https://github.com/datafusion-contrib/datafusion-orc/issues/9)**
    - Bump arrow to 47.0 ([`0348b46`](https://github.com/datafusion-contrib/datafusion-orc/commit/0348b466a47c6701fef107e78c353eeeedea59d7))
 * **Uncategorized**
    - Add explicit path for benches ([`a850c52`](https://github.com/datafusion-contrib/datafusion-orc/commit/a850c52f29d7036611263166ab35fb8be0ab98a9))
    - Add explicit path for examples ([`f9c47e7`](https://github.com/datafusion-contrib/datafusion-orc/commit/f9c47e78a90990780f1aeb61259620220592cb59))
    - Empty changelog ([`73cd5ea`](https://github.com/datafusion-contrib/datafusion-orc/commit/73cd5ea85511bc9cae0affa94b56cac8ea29288a))
    - Update README on Union limitation ([`4f1c98d`](https://github.com/datafusion-contrib/datafusion-orc/commit/4f1c98de1744033899f8f5a0c61a32eeffc5db25))
    - Fix crate name usages ([`c9c4fca`](https://github.com/datafusion-contrib/datafusion-orc/commit/c9c4fca424690ee7987468d542967a3dfffdbbb8))
    - Update documentation and cleanup root level files ([`49f5001`](https://github.com/datafusion-contrib/datafusion-orc/commit/49f5001e9988a2b8441354beebd8ce902bd343bf))
    - Cleanup tests ([`a5e952d`](https://github.com/datafusion-contrib/datafusion-orc/commit/a5e952df54294b30cdcc8a0b451ec3270e6c73b8))
    - Typo ([`79a7532`](https://github.com/datafusion-contrib/datafusion-orc/commit/79a753244d4f3a09aa15490765e1097763ea1607))
    - Enable test_seek integration test ([`1493e67`](https://github.com/datafusion-contrib/datafusion-orc/commit/1493e67c3c8bf24f73501f410e0daf2947b78f23))
    - Support decoding Union with <= 127 variants into Sparse UnionArrays ([`ee69b91`](https://github.com/datafusion-contrib/datafusion-orc/commit/ee69b91cb2ce4c18ee5148bd541658d362fc577f))
    - Comment out failing orc_11_format integration test ([`0de422f`](https://github.com/datafusion-contrib/datafusion-orc/commit/0de422fb47d82a9871bdf8b878dd0e7ba94492a1))
    - Update comments ([`6d0e58a`](https://github.com/datafusion-contrib/datafusion-orc/commit/6d0e58afbc2b47be493e65209fb29ad9bd47ee4e))
    - Display file format version with orc-metadata ([`f76a7b2`](https://github.com/datafusion-contrib/datafusion-orc/commit/f76a7b22e4a6ed5b35f110a5fa8849db2ee68dce))
    - Refactor to consistent mod structure ([`d8e6684`](https://github.com/datafusion-contrib/datafusion-orc/commit/d8e6684a28bee0f814a930c21d5912d332532fc3))
    - Fix TIMESTAMP to align with ORC impl ([`60288cd`](https://github.com/datafusion-contrib/datafusion-orc/commit/60288cdee57f72dec84c3fd9f6085561568aad49))
    - Add Tz to Stripe ([`3369b5d`](https://github.com/datafusion-contrib/datafusion-orc/commit/3369b5d7bb110dafde64288416c86bc8c996e465))
    - Refactor Stripe ([`8c0fe49`](https://github.com/datafusion-contrib/datafusion-orc/commit/8c0fe49a5e4c5681004c5a9a83b8649d4db3bc48))
    - Support TIMESTAMP_INSTANT ([`1888015`](https://github.com/datafusion-contrib/datafusion-orc/commit/18880157be312f4720f1a6f3b961a87abcfae6a7))
    - Update Spark test data and add PyArrow timestamp data generator ([`26d73bd`](https://github.com/datafusion-contrib/datafusion-orc/commit/26d73bd11b067086513cea45d635b2de769c12ac))
    - Enhance orc-metadata bin to show basic stripe metadata ([`24bd57e`](https://github.com/datafusion-contrib/datafusion-orc/commit/24bd57ecf49f710c40d6915321e5245021ed41fb))
    - Initial orc-metadata CLI tool ([`8537adb`](https://github.com/datafusion-contrib/datafusion-orc/commit/8537adb901fd990796b2175a1f97f46982307b68))
    - Minor refactor ([`ff0f36e`](https://github.com/datafusion-contrib/datafusion-orc/commit/ff0f36e6cc32428868e7209058b4d266f08e024e))
    - Enable over1k_bloom integration test and add comments ([`cd375d5`](https://github.com/datafusion-contrib/datafusion-orc/commit/cd375d59cfbe67055826d438ec31273c21c64046))
    - Edge case where required streams may be missing ([`1fe21b5`](https://github.com/datafusion-contrib/datafusion-orc/commit/1fe21b5ce8278e953ba3d06086a5fd0274f92199))
    - Enable empty_file integration test by fixing MapArray children names ([`04a5050`](https://github.com/datafusion-contrib/datafusion-orc/commit/04a505078b930f1aa3caf46e3a083b09dfef572c))
    - Enable test1 integration test by fixing MapArray children names ([`e56f1de`](https://github.com/datafusion-contrib/datafusion-orc/commit/e56f1defeae5caeff9ec64d3066fe3a026d09550))
    - Update comments ([`c613c71`](https://github.com/datafusion-contrib/datafusion-orc/commit/c613c711b68762edce22909fc979297d0a738d61))
    - Align Map Arrow datatype derivation with MapArrayDecoder ([`f15848c`](https://github.com/datafusion-contrib/datafusion-orc/commit/f15848c4fa51410872315f6c0260c541e1392b21))
    - Compare concatenated RecordBatches in integration tests ([`0405e23`](https://github.com/datafusion-contrib/datafusion-orc/commit/0405e23a291ead841353a182aab1338bd7b0c8cf))
    - Decimal support ([`bb885c0`](https://github.com/datafusion-contrib/datafusion-orc/commit/bb885c03cfedb4d4e16d0203b90a9789463e2fb7))
    - Minor refactor and comments ([`70a0262`](https://github.com/datafusion-contrib/datafusion-orc/commit/70a02628f06922bbd52259b98ecf214c171e8a8a))
    - Cast dictionary encoded string column stripes to regular StringArray ([`7f66552`](https://github.com/datafusion-contrib/datafusion-orc/commit/7f6655245d6b384e610e690d86484f12b17849bf))
    - Simplify Float trait bounds ([`1ecfdef`](https://github.com/datafusion-contrib/datafusion-orc/commit/1ecfdeffb9a8866ca82c0918a36676ca5248fbd0))
    - Unit tests for FloatIter ([`9f7bbc4`](https://github.com/datafusion-contrib/datafusion-orc/commit/9f7bbc4f60aefefc71c958b5b22943089c4a38ff))
    - Rename decode.rs to decode/mod.rs ([`9f6e282`](https://github.com/datafusion-contrib/datafusion-orc/commit/9f6e282498dd93186f4bfe1373389a818283175d))
    - Remove variable_length.rs ([`d762305`](https://github.com/datafusion-contrib/datafusion-orc/commit/d76230596610fd5ccea1d3ffb6229446c86d2d49))
    - Light refactoring ([`5e49353`](https://github.com/datafusion-contrib/datafusion-orc/commit/5e49353f47d7f592d9ce55ca2daa244808aeae01))
    - Add minimal example of integration with DataFusion ([`8c68f47`](https://github.com/datafusion-contrib/datafusion-orc/commit/8c68f472a24c7ff12401ccdb3704991f0c7d080d))
    - Comment ([`8909d01`](https://github.com/datafusion-contrib/datafusion-orc/commit/8909d014b5f72c14f2088601da82c6b0ac3d33e4))
    - Remove NullableIterator in favour of explicitly handling an optional Present stream ([`61e1cbf`](https://github.com/datafusion-contrib/datafusion-orc/commit/61e1cbf529b33594a6c4a9ede26c1c31ab74a609))
    - Replace usage of new_present_iter in binary decoder ([`4c91b13`](https://github.com/datafusion-contrib/datafusion-orc/commit/4c91b13dce633e213348dbddea4ed5e6825e1f77))
    - Refactor common code ([`933735f`](https://github.com/datafusion-contrib/datafusion-orc/commit/933735f21fea15be2fb83c41234ac9969b9681f0))
    - Introduce get_present_vec to optionally get present stream ([`e9f85f2`](https://github.com/datafusion-contrib/datafusion-orc/commit/e9f85f2c99c65e2ee56fdfda8bff5c2847c3117a))
    - Don't return Option in ArrayBatchDecoder::next_batch ([`1489ebf`](https://github.com/datafusion-contrib/datafusion-orc/commit/1489ebfb293b858948ad2a3b847a5dd778bd19b5))
    - Consolidate binary decoding logic with strings ([`74a864f`](https://github.com/datafusion-contrib/datafusion-orc/commit/74a864faf4d4bc3ad9da85d4304e348138f36a2c))
    - Refactor string column handling to read contents directly to StringArrays ([`93b37e6`](https://github.com/datafusion-contrib/datafusion-orc/commit/93b37e603a584f7946145a9446b4b57a5a1417a7))
    - Centralize string column decoding into decoder/string.rs ([`dd536d6`](https://github.com/datafusion-contrib/datafusion-orc/commit/dd536d611550520331133c1f087cd0287f34d1c5))
    - Refactor to dyn array trait based column decoders ([`574886d`](https://github.com/datafusion-contrib/datafusion-orc/commit/574886d1360fbab6781ad56f56b6c738856221a3))
    - Simplify NullableIterator::next ([`5a6fdca`](https://github.com/datafusion-contrib/datafusion-orc/commit/5a6fdca1dacb0e893ed2205e9100914090a441c9))
    - Don't return Option in NullableIterator::collect_chunk ([`6af38b4`](https://github.com/datafusion-contrib/datafusion-orc/commit/6af38b442d19a809f98dd2039a4e8d715c88b19c))
    - Move number_of_rows from Column to Stripe ([`09fd1d7`](https://github.com/datafusion-contrib/datafusion-orc/commit/09fd1d734b07f4f2b93625ba65509f382d3da01c))
    - Refactor NInt from_be_bytes to use associated Bytes type ([`8625a0d`](https://github.com/datafusion-contrib/datafusion-orc/commit/8625a0d06acec5e15a15fe622acb0fcc610fda31))
    - Replace float iter macro with generics ([`b199319`](https://github.com/datafusion-contrib/datafusion-orc/commit/b199319fd07230cb1801256a2bc02e68453163c8))
    - Remove bool return from Decoder::append_value ([`6b1f41e`](https://github.com/datafusion-contrib/datafusion-orc/commit/6b1f41e64486f831c5c3faef6870d8025b06eb1c))
    - Split up arrow_reader.rs, use mod.rs pattern ([`6d5a4ba`](https://github.com/datafusion-contrib/datafusion-orc/commit/6d5a4ba0fd3104385aaeba41b6c30d9a1512d208))
    - Simplify fetching RLE iterator ([`d814c9e`](https://github.com/datafusion-contrib/datafusion-orc/commit/d814c9ec4ecda2621ad7acee06aac20ecbc964a3))
    - Remove unused integer decoding impl's ([`0ff03e0`](https://github.com/datafusion-contrib/datafusion-orc/commit/0ff03e037470c8f45421005e0b983bc6390c5086))
    - Use specialized Int decoders when decoding Integer columns ([`f1eb778`](https://github.com/datafusion-contrib/datafusion-orc/commit/f1eb7783aa1f9ea4b56f1a03932c30476a68d0d6))
    - Use assert_batches_eq in tests ([`d8c512f`](https://github.com/datafusion-contrib/datafusion-orc/commit/d8c512fe4216834869a5fc60e1b5aa1903d1e3eb))
    - Merge pull request #37 from datafusion-contrib/WenyXu-patch-1 ([`fd1bc01`](https://github.com/datafusion-contrib/datafusion-orc/commit/fd1bc0150c0123e9b25d70f5ad52346abf42bc05))
    - Update README.md ([`403d8f0`](https://github.com/datafusion-contrib/datafusion-orc/commit/403d8f0042d9ee93ef257f45078a6036369b52ac))
    - Merge pull request #32 from WenyXu/feat/map ([`c89535a`](https://github.com/datafusion-contrib/datafusion-orc/commit/c89535a7db944a07b538ee28739f0be19a70c1f2))
    - Support to map datatype ([`424b021`](https://github.com/datafusion-contrib/datafusion-orc/commit/424b02140b2d3cfa93afa7479cb1f6a29dbd9365))
    - Merge pull request #30 from WenyXu/feat/list ([`85baf80`](https://github.com/datafusion-contrib/datafusion-orc/commit/85baf804baf049eb0c97554f50dc9c20e25db2a7))
    - Apply suggestions from CR ([`b78e9da`](https://github.com/datafusion-contrib/datafusion-orc/commit/b78e9da99bd7ecfcfefd3d1d5cd66736990d658e))
    - Support to list datatype ([`6a7267e`](https://github.com/datafusion-contrib/datafusion-orc/commit/6a7267e4f8080958faa6ebba7bc9f3b9f0e321f2))
    - Merge pull request #29 from WenyXu/refactor/refactor-boolean-iter ([`cd2dc0e`](https://github.com/datafusion-contrib/datafusion-orc/commit/cd2dc0e63908f2e6657c24b0efbf031694bf7e2e))
    - Refactor byte/boolean iter ([`d438c1d`](https://github.com/datafusion-contrib/datafusion-orc/commit/d438c1dd855f2f553507cd3cafcb7fe559298186))
    - Merge pull request #28 from datafusion-contrib/WenyXu-patch-1 ([`51059f8`](https://github.com/datafusion-contrib/datafusion-orc/commit/51059f8358a54f2626ad20e921ed27e7f1f38c20))
    - Update README.md ([`0075828`](https://github.com/datafusion-contrib/datafusion-orc/commit/00758286222c6f35cd5761fdf5ac4915f769cc0b))
    - Merge pull request #26 from WenyXu/feat/struct ([`b641249`](https://github.com/datafusion-contrib/datafusion-orc/commit/b64124955e6250edf132e8ad9c3c7e24d5ade3ee))
    - Apply suggestions from CR ([`feb932c`](https://github.com/datafusion-contrib/datafusion-orc/commit/feb932c0ea21a79d58f6ba61749637839e579e2a))
    - Support to struct datatype ([`b4828e4`](https://github.com/datafusion-contrib/datafusion-orc/commit/b4828e49705b21216d4828c3d4c1dd446cdd88cd))
    - Merge pull request #25 from datafusion-contrib/fix/read_entire_file ([`de88a24`](https://github.com/datafusion-contrib/datafusion-orc/commit/de88a24961be3e54c98085c8f9baf9af76ab5e70))
    - Add async test ([`e0aa734`](https://github.com/datafusion-contrib/datafusion-orc/commit/e0aa734ca2acc59183d4eafcdf789cc0a49ff665))
    - Fix bug where not reading all stripes in file ([`a5defdc`](https://github.com/datafusion-contrib/datafusion-orc/commit/a5defdc9c5446e39521de455d7265e16f54741bb))
    - Merge pull request #22 from WenyXu/feat/support-tiny-int ([`f158593`](https://github.com/datafusion-contrib/datafusion-orc/commit/f158593fe36b6000ac09d77e07ab3c3998122fa4))
    - Apply suggestions from CR ([`995e0f3`](https://github.com/datafusion-contrib/datafusion-orc/commit/995e0f34aa6e6a89c1c5f10fecf124344db3e753))
    - Map tinyint to i8 ([`3378874`](https://github.com/datafusion-contrib/datafusion-orc/commit/3378874ee7b60491c184254cad613eddd630fa5c))
    - Support to read tinyint ([`cf13577`](https://github.com/datafusion-contrib/datafusion-orc/commit/cf13577180e414c96a77588554231d0842361768))
    - Merge pull request #21 from datafusion-contrib/chore/update_readme_decompression ([`0acbc07`](https://github.com/datafusion-contrib/datafusion-orc/commit/0acbc07dc6a8ab433daa1ea37a564f03865873d9))
    - Update README to reflect decompression support ([`2b4d2f0`](https://github.com/datafusion-contrib/datafusion-orc/commit/2b4d2f07c60006519154d5499a117517f60e78b5))
    - Add comments for tracking the provenance ([`aa29e38`](https://github.com/datafusion-contrib/datafusion-orc/commit/aa29e38147616b18ee20a8d379e3bb8f8cf17f53))
    - Update Cargo.toml ([`fc2a9c3`](https://github.com/datafusion-contrib/datafusion-orc/commit/fc2a9c374c74492a8fa8e44cf6fc6afbb1fcc5e9))
    - Remove MIT LICENSE ([`fa40696`](https://github.com/datafusion-contrib/datafusion-orc/commit/fa406969df9868ae2bf2371ad549a519ae12f7ab))
    - Bump version to 0.2.41 ([`03cfc05`](https://github.com/datafusion-contrib/datafusion-orc/commit/03cfc053c33dcb481a5d1bf9fc9b8af56b0a73b7))
    - Bump arrow to 43.0 ([`5596dc3`](https://github.com/datafusion-contrib/datafusion-orc/commit/5596dc34b47d6b9f000a327cf659af5138745f8e))
    - Bump version to 0.2.4 ([`a8cf57a`](https://github.com/datafusion-contrib/datafusion-orc/commit/a8cf57a0b8f71ae3a95770a0519245ae698f1a59))
    - Update README.md ([`ccba511`](https://github.com/datafusion-contrib/datafusion-orc/commit/ccba51184f380d4bb478e4f9fb980cb6e8cffa4d))
    - Support to zstd compression ([`a2c8ae5`](https://github.com/datafusion-contrib/datafusion-orc/commit/a2c8ae529ef252129784d28af70e5b36f27172fd))
    - Bump version to 0.2.3 ([`5840708`](https://github.com/datafusion-contrib/datafusion-orc/commit/5840708dfb65a12b1568cc266a94105c488becb0))
    - Remove StripeFactoryInner ([`e037b14`](https://github.com/datafusion-contrib/datafusion-orc/commit/e037b1406755712470731edc439496fe6fe37a90))
    - Bump version to 0.2.2 ([`628680c`](https://github.com/datafusion-contrib/datafusion-orc/commit/628680c6ca6ad7f05c6cf3e4ac89aef0ce52acb7))
    - Fmt toml ([`ac249e4`](https://github.com/datafusion-contrib/datafusion-orc/commit/ac249e4a30c8d261dbf6351950c3794277842ced))
    - Make ArrowStreamReader sendable ([`875a08d`](https://github.com/datafusion-contrib/datafusion-orc/commit/875a08d9258e2e1eb6e6eed1c0426e8739c7aa27))
    - Make create_arrow_schema return Schema ([`90d2deb`](https://github.com/datafusion-contrib/datafusion-orc/commit/90d2debeda6b7625f36a5c411e065f0c76d45ccb))
    - Downgrade arrow version ([`74ea42f`](https://github.com/datafusion-contrib/datafusion-orc/commit/74ea42fd8879c5c5ca580271f282f4816aa15f54))
    - Add schema method for ArrowStreamReader ([`ce65c58`](https://github.com/datafusion-contrib/datafusion-orc/commit/ce65c5881ce8765d1a9cd484b980b8d98ca4b86b))
    - Bump version to 0.2.0 ([`3c9c62e`](https://github.com/datafusion-contrib/datafusion-orc/commit/3c9c62e6aab505dd666e63dc5e6f892d7c7f04b2))
    - Update README.md ([`9edb609`](https://github.com/datafusion-contrib/datafusion-orc/commit/9edb6095705d4d590e67961ee0068ffd458aa5fa))
    - Add async arrow reader ([`58af788`](https://github.com/datafusion-contrib/datafusion-orc/commit/58af78886b26673ce523f201537c7dac78b5a3d7))
    - Move arrow to arrow reader ([`d5b1542`](https://github.com/datafusion-contrib/datafusion-orc/commit/d5b1542dc4093264d308be1675803e363bcccafe))
    - Rle v2 reader ([`c43a8c5`](https://github.com/datafusion-contrib/datafusion-orc/commit/c43a8c5efc5c4fdc13bf7f3d818fc42b6518d525))
    - Add pathed base rle v2 test ([`6a34224`](https://github.com/datafusion-contrib/datafusion-orc/commit/6a342245b9cac221ab7ab644c64f6dfa99f4fe56))
    - Udpate Cargo.toml ([`2231475`](https://github.com/datafusion-contrib/datafusion-orc/commit/223147575cca59d4afa5eb0531be60cdd5ec373b))
    - Test Cursor::root ([`a0704a4`](https://github.com/datafusion-contrib/datafusion-orc/commit/a0704a4dac680a18e049b3855a9dd38f5b12a04e))
    - Update README.md ([`da265db`](https://github.com/datafusion-contrib/datafusion-orc/commit/da265dbb8f74913227961b8a127e78521b97df32))
    - Add tests ([`2275cb2`](https://github.com/datafusion-contrib/datafusion-orc/commit/2275cb2e53192fe11afe93d5e9971c3b5ad60c8b))
    - Add test data ([`616fac9`](https://github.com/datafusion-contrib/datafusion-orc/commit/616fac90b0027909328cfeec36da0a52f07b3b9e))
    - Add README.md ([`8b99d47`](https://github.com/datafusion-contrib/datafusion-orc/commit/8b99d475825c7233496d9f3d36aa997101dc3ab6))
    - Add licenses ([`4757291`](https://github.com/datafusion-contrib/datafusion-orc/commit/47572917cf51a8198733645b071fded8f59ff5c4))
    - Add ci cfg ([`c9b2699`](https://github.com/datafusion-contrib/datafusion-orc/commit/c9b269937f24bb33eb03391fa174024db69bd10c))
    - Init implementation ([`3ed1163`](https://github.com/datafusion-contrib/datafusion-orc/commit/3ed1163853975d3a722b9b54717ceea9d0eb7fce))
    - Initial commit ([`340139f`](https://github.com/datafusion-contrib/datafusion-orc/commit/340139fabdf82bfaff7a8b8856d44db8bfc7a7df))
</details>

