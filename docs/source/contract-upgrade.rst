Upgrading Contracts
===================
While every care is taken in development of contract code, 
inevitably upgrades will be required to fix bugs (in either of design or implementation). 
Upgrades can involve a substitution of one version of the contract code for another or changing 
to a different contract that understands how to migrate the existing state objects. State objects 
refer to the contract code (by hash) they are intended for, and even where state objects can be used 
with different contract versions, changing this value requires issuing a new state object.

Workflow
--------
The current design for how upgrades would work looks like this:

1. Two banks, A and B negotiate a trade, off-platform

2. Banks A and B execute a protocol to construct a state object representing the trade, using contract X, and include it in a transaction (which is then signed and sent to the Uniqueness Service).

3. Time passes.

4. The developer of contract X discovers a bug in the contract code, and releases a new version, contract Y.

5. At this point of time all nodes should stop issuing contract X.

6. Banks A and B review the new contract via standard change control processes and identify the contract states they agreed to upgrade, they can decide not to upgrade some contract states as they might be needed for other obligation contract.

7. Banks A and B instruct their Corda nodes (via RPC) to be willing to upgrade state objects of contract X, to state objects for contract Y using agreed upgrade path.

8. One of the parties [Instigator] initiates an reissue of state objects referring to contract X, to a new state object referring to contract Y.

9. A proposed transaction [Proposal], taking in the old state and outputting the reissued version, is created and signed with the node's private key.

10. The node [Instigator] sends the proposed transaction, along with details of the new contract upgrade path it's proposing, to all participants of the state object.

11. Each counterparty [Acceptor] verifies the proposal, signs or rejects the state reissuance accordingly, and sends a signature or rejection notification back to the initiating node.

12. If signatures are received from all parties, the initiating node assembles the complete signed transaction and sends it to the consensus service.


Authorising upgrade
-------------------
Each of the participants in the upgrading contract will have to instruct their node that they are willing to upgrade the state object before the upgrade.
Currently the vault service is used to manage the authorisation records. User will be able to use RPC to perform such instructions.

.. container:: codeset

   .. sourcecode:: kotlin
   
       /**
        * Authorise a contract upgrade for a contract state.
        */
       fun authoriseContractUpgrade(state: StateAndRef<*>, upgrade: UpgradedContract<ContractState, ContractState>)


Proposing an upgrade
--------------------
After all parties have registered the intention of upgrading the contract state, one of the contract participant can initiate the upgrade process by running the contract upgrade flow.
The Instigator will create a new state and sent to each participant for signatures, each of the participants (Acceptor) will verify and sign the proposal and returns to the instigator.
The transaction will be notarised and persisted once every participant verified and signed the upgrade proposal.
