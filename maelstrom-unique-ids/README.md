# Gossip Gloomers - Solutions - Challenge 2
## Unique IDs

This is the solution to Challenge 2.
I would recommend you to checkout my detailed [writeup](https://medium.com/@burnerlee/gossip-gloomers-a-detailed-walkthrough-challenge-2-fe1dc7a63496) on medium.

There are several approaches to the problem:
1. Totally available and Partition tolerant system
    - sol-1A - Segmented ID Generation
    - sol-1B - Timestamp ID Generation
2. Consistent state system with total availability (Additional Solution - but no partition tolerance):
    - sol-2 - CAS Communication b/w nodes
