# RWRHN-FF

RWRHN-FF is a semi-supervised algorithm. It integrates various types of data into a heterogeneous network via type-two fuzzy and implements a random walk with restart on heterogeneous network algorithm to find new nodes. In this method, first, four gene-gene similarity networks are constructed based on different genomic sources and then integrated using the type-II fuzzy voter scheme. The resulting gene-gene network is then linked to a disease-disease similarity network, which itself is constructed by the integration of four sources, through a two-part disease-gene network. The product of this process is a reliable heterogeneous network, which is analyzed by the RWRHN algorithm. All steps have mentioned as following:

First, for construct gene-gene similarity networks use four networks (Gene-Gene.rar file include all networks, also, there are genes id in GeneId.rar file).

Second, use data in Drug-Repositioning-Data repo (https://github.com/dkrlab/Drug-Repositioning-Data) to construct a disease-disease network.

Third, with voter (in Voter.rar file), you can integrate gene-gene networks, be aware that your own should configure the membership functions as a type-II fuzzy.

Fourth, using disease-gene data in Drug-Repositioning-Data repo, link the gene-gene network to the disease-disease network, which causes a heterogeneous network.

Fifth, in order to implement a parallel ranking, run spark code on Apache Spark.










