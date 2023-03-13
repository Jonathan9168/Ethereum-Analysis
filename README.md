# Ethereum-Analysis
```
Analysing Ethereum blockchain data using a series of map reduce jobs as part of ECS640U coursework [Big Data Processing]
```
### Dataset - Blocks CSV File

| Column Name           | Decription                                                   | Example              |
| --------------------- | ------------------------------------------------------------ | -------------------- |
| **number**            | The block number                                             | 4776199              |
| **hash**              | Hash of the block                                            | 0x9172600443ac88e... |
| **miner**             | The address of the beneficiary to whom the mining rewards were given | 0x5a0b54d5dc17e0a... |
| **difficulty**        | Integer of the difficulty for this block                     | 1765656009004680     |
| **size**              | The size of this block in bytes                              | 9773                 |
| **gas_limit**         | The maximum gas allowed in this block                        | 7995996              |
| **gas_used**          | The total used gas by all transactions in this block         | 2042230              |
| **timestamp**         | The timestamp for when the block was collated                | 1513937536           |
| **transaction_count** | The number of transactions in the block                      | 62                   |

### Dataset - Transactions CSV File

| Column Name         | Decription                                                   | Example              |
| ------------------- | ------------------------------------------------------------ | -------------------- |
| **block_number**    | Block number where this transaction was in                   | 6638809              |
| **from_address**    | Address of the sender                                        | 0x0b6081d38878616... |
| **to_address**      | Address of the receiver. null when it is a contract creation transaction | 0x412270b1f0f3884... |
| **value**           | Value transferred in Wei (the smallest denomination of ether) | 240648550000000000   |
| **gas**             | Gas provided by the sender                                   | 21000                |
| **gas_price**       | Gas price provided by the sender in Wei                      | 5000000000           |
| **block_timestamp** | Timestamp the associated block was registered at (effectively timestamp of the transaction) | 1541290680           |

### Dataset - Contracts CSV File

| Column Name         | Decription                                                   | Example              |
| ------------------- | ------------------------------------------------------------ | -------------------- |
| **address**         | Address of the contract                                      | 0x9a78bba29a2633b... |
| **is_erc20**        | Whether this contract is an ERC20 contract                   | false                |
| **is_erc721**       | Whether this contract is an ERC721 contract                  | false                |
| **block_number**    | Block number where this contract was created                 | 8623545              |
| **block_timestamp** | Timestamp the associated block was registered at (effectively timestamp of the transaction) | 2019-09-26 08:50:... |
