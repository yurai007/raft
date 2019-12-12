import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlin.system.measureTimeMillis

/* Two Phase Commit
   ref: https://www.the-paper-trail.org/post/2008-11-27-consensus-protocols-two-phase-commit/
   TODO1: why lack of launches arround run-s cause very strange behaviour (10s timeout without any print :) ?
   TODO2: kotlinc -Xuse-experimental=kotlin.Experimental 2pc.kt -d 2pc.jar
   error: unresolved reference: kotlinx
*/

enum class State {
    INITIAL, PROPOSE, VOTE, COMMIT_OR_ABORT
}

abstract class Node {
    abstract suspend fun run()
    var register: Int = 0
    var state: State = State.INITIAL
}

class Replica(shouldCommit : Boolean) : Node() {
    override suspend fun run() {
        assert(state == State.INITIAL)
        val proposedValue = proposeFromLeader()
        state = State.PROPOSE
        vote(shouldCommit)
        state = State.VOTE
        val commitReply = commitOrAbortFromLeader()
        state = State.COMMIT_OR_ABORT
        if (commitReply) {
            register = proposedValue
            println("Replica: Value := $register")
        } else {
            println("No consensus")
        }
    }

    suspend fun commitOrAbortFromLeader() = channelToLeader.receive().toBoolean()

    suspend fun commitOrAbort(value : Boolean) = channelToLeader.send(value.toInt())

    suspend fun voteFromReplica() : Boolean = channelToLeader.receive().toBoolean()

    suspend fun vote(value : Boolean) = channelToLeader.send(value.toInt())

    suspend fun proposeFromLeader() : Int = channelToLeader.receive()

    suspend fun propose(value : Int) = channelToLeader.send(value)

    fun Boolean.toInt() = if (this) 1 else 0
    fun Int.toBoolean() = if (this == 1) true else false

    val shouldCommit = shouldCommit
    val channelToLeader = Channel<Int>()
}

class Leader(replicas : List<Replica>, proposed : Int) : Node() {
    override suspend fun run() {
        assert(state == State.INITIAL)
        state = State.PROPOSE
        replicas.forEach {it.propose(proposedValue)}
        state = State.VOTE
        val votes = mutableListOf<Boolean>()
        replicas.forEach {votes.add(it.voteFromReplica())}
        state = State.COMMIT_OR_ABORT
        val maybeCommit = !votes.contains(false)
        replicas.forEach {it.commitOrAbort(maybeCommit)}
        if (maybeCommit) {
            register = proposedValue
            println("Leader: Value := $register")
        } else {
            println("No consensus")
        }
        state = State.INITIAL
    }

    val replicas = replicas
    val proposedValue = proposed
}

fun oneLeaderOneReplicaScenarioWithConsensus() = runBlocking<Unit> {
    val replica = Replica(true)
    val leader = Leader(listOf(replica), 123)
    launch { replica.run() }
    launch { leader.run() }
}

fun oneLeaderOneReplicaScenarioNoConsensus() = runBlocking<Unit> {
    val replica = Replica(false)
    val leader = Leader(listOf(replica), 123)
    launch { replica.run() }
    launch { leader.run() }
}

fun oneLeaderReplicasScenarioWithConsensus() = runBlocking<Unit> {
    val replicas = listOf(Replica(true), Replica(true), Replica(true), Replica(true), Replica(true), Replica(true), Replica(true),
                         Replica(true), Replica(true), Replica(true), Replica(true), Replica(true), Replica(true))
    val leader = Leader(replicas, 321)
    launch { leader.run() }
    replicas.forEach { launch { it.run() } }
}

fun oneLeaderReplicasScenarioWithNoConsensus() = runBlocking<Unit> {
    val replicas = listOf(Replica(true), Replica(true), Replica(true), Replica(true), Replica(true), Replica(true), Replica(true),
                         Replica(true), Replica(true), Replica(true), Replica(true), Replica(true), Replica(true), Replica(false))
    val leader = Leader(replicas, 321)
    launch { leader.run() }
    replicas.forEach { launch { it.run() } }
}

fun main(args: Array<String>) {
    oneLeaderReplicasScenarioWithConsensus()
    oneLeaderReplicasScenarioWithNoConsensus()
}
