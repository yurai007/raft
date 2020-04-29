import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlin.math.*
import kotlin.collections.mutableListOf
import java.time.LocalDateTime

// ref: https://raft.github.io/raft.pdf

enum class State {
    FOLLOWER_INITIAL, FOLLOWER_DONE, CANDIDATE, LEADER_INITIAL, LEADER_DONE
}

interface Message
data class HeartBeat(val done : Boolean = false) : Message
data class AppendEntriesReq(val metaEntry : MetaEntry, val term : Int, val prevIndex : Int, val prevTerm : Int, val leaderCommit : Int) : Message
data class AppendEntriesResp(val term : Int, val success : Boolean) : Message
data class RequestVoteReq(val term : Int, val candidateId : Int, val lastLogIndex : Int, val lastLogTerm : Int) : Message
data class RequestVoteResp(val term : Int, val voteGranted : Boolean) : Message
data class MetaEntry(val entry : Pair<Char, Int>, val term : Int)

abstract class Node() {

    constructor(logState : HashMap<Char, Int>, log : MutableList<MetaEntry>, state : State) : this() {
        this.logState = logState
        this.log = log
        this.state = state
    }

    abstract suspend fun run()
    open fun me() : Node = this

    fun trackLog() {
        if (debug) {
            println("$log")
        }
    }

    protected var logState = HashMap<Char, Int>()
    protected var log = mutableListOf<MetaEntry>()
    var state: State = State.FOLLOWER_INITIAL

    var currentTerm = 0
    var commitIndex = 0
    protected var lastApplied = 0
    private val debug = true
    protected val rpcTimeoutMs : Long = 50
}

open class Follower() : Node() {

    constructor(logState : HashMap<Char, Int>, log : MutableList<MetaEntry>, state : State) : this()  {
        super.logState = logState
        super.log = log
        super.state = state
    }

    override suspend fun run() {
        println("Follower $this: start")
        state = State.FOLLOWER_INITIAL
        currentTerm++
        commitIndex = max(log.size, 0)
        var maybeHeartBeat = receiveHeartbeat()
        var maybeAppendEntries : AppendEntriesReq?
        while (maybeHeartBeat != null && !done(maybeHeartBeat!!)) {
            maybeAppendEntries = receiveAppendEntriesReq()
            if (maybeAppendEntries == null)
                break
            val appendEntries = maybeAppendEntries!!
            var apply = true
            val prevIndex = appendEntries.prevIndex
            val prevTerm = appendEntries.prevTerm
             // [1]
            if (appendEntries.term < currentTerm) {
                apply = false
            } else {
                // [1]
                if (appendEntries.term > currentTerm) {
                    currentTerm = appendEntries.term
                }
                if (prevIndex >= 0) {
                    if (prevIndex < log.size) {
                        // [2]
                        if (log[prevIndex].term != prevTerm) {
                            apply = false
                        }
                    } else {
                        apply = false
                    }
                    if (prevIndex + 1 < log.size) {
                        // [3]
                        if (log[prevIndex + 1].term != appendEntries.metaEntry.term) {
                            // cleanup because of inconsistency, leave only log[0..prevIndex] prefix,
                            // with assumption that prefix is valid we can append entry in this iteration
                            shrinkUntil(prevIndex)
                        }
                    }
                }
            }
            sendAppendEntriesResp(AppendEntriesResp(currentTerm, apply))
            val metaEntry = appendEntries.metaEntry
            val leaderCommit = appendEntries.leaderCommit
            val (id, value) = metaEntry.entry
            if (apply) {
                // [4]
                log.add(MetaEntry(metaEntry.entry, metaEntry.term))
                logState[id] = value
                println("Follower $this: $id := $value")
                lastApplied++
                // [5]
                commitIndex = min(leaderCommit + 1, commitIndex + 1)
            } else {
                println("Follower $this: no consensus for $id")
                trackLog()
            }
            maybeHeartBeat = receiveHeartbeat()
        }
        if (maybeHeartBeat != null && maybeHeartBeat!!.done) {
            state = State.FOLLOWER_DONE
            println("Follower $this: done with commitIndex=$commitIndex")
            trackLog()
        } else {
            state = State.CANDIDATE
            println("Follower $this: Heartbeat or AppendEntriesReq failed with election timeout. Start election.")
        }
    }
    // slice
    private fun shrinkUntil(index : Int) {
        for (i in log.size-1 downTo index + 1) {
            val (keyValue, _) = log[i]
            val (key, _) = keyValue
            logState.remove(key)
            log.removeAt(i)
        }
    }

    fun verifyLog(expectedLog : MutableList<MetaEntry> ) : Boolean = expectedLog == log

    suspend fun sendHeartbeat(done : Boolean) : Boolean? {
        return withTimeoutOrNull(rpcTimeoutMs) {
            channelToLeader.send(HeartBeat(done))
            true
        }
    }
    suspend fun sendAppendEntriesReq(entriesReq : AppendEntriesReq) : Boolean? {
        return withTimeoutOrNull(rpcTimeoutMs) {
            channelToLeader.send(entriesReq)
            true
        }
    }
    private suspend fun sendAppendEntriesResp(entriesResp : AppendEntriesResp) : Boolean? {
        return withTimeoutOrNull(rpcTimeoutMs) {
            channelToLeader.send(entriesResp)
            true
        }
    }

    private suspend fun receiveAppendEntriesReq() : AppendEntriesReq? {
        val message : Message? = withTimeoutOrNull(rpcTimeoutMs) { channelToLeader.receive() }
        return message as? AppendEntriesReq
    }
    suspend fun receiveAppendEntriesResp() : AppendEntriesResp? {
        val message : Message? = withTimeoutOrNull(rpcTimeoutMs) { channelToLeader.receive() }
        return message as? AppendEntriesResp
    }
    private suspend fun receiveHeartbeat() : HeartBeat? {
        val message : Message? = withTimeoutOrNull(rpcTimeoutMs) { channelToLeader.receive() }
        return message as? HeartBeat
    }
    private fun done(heartBeat : HeartBeat) : Boolean = heartBeat.done

    private val channelToLeader = Channel<Message>()
}

class Leader(followers : List<Follower>, entriesToReplicate : HashMap<Char, Int>) : Node() {

    constructor(followers : List<Follower>, entriesToReplicate : HashMap<Char, Int>,
                logState : HashMap<Char, Int>, log : MutableList<MetaEntry>, state : State) : this(followers, entriesToReplicate)  {
        super.logState = logState
        super.log = log
        super.state = state
    }

    override suspend fun run() {
        state = State.LEADER_INITIAL
        currentTerm++
        println("Leader of term $currentTerm")
        entriesToReplicate.forEach { (id, value) ->
            followers.forEach {
                if (sendHeartbeat(it, false) == null) {
                    // FIXME: fallback here and there is not the proper way to handle slow Follower
                    return fallbackTo(State.CANDIDATE)
                }
            }
            var entry = MetaEntry(Pair(id, value), currentTerm)
            lastApplied++
            // [5.3]
            log.add(entry)

            followers.forEach {
                do {
                    var followerIsDone = false
                    val prevIndex = min(replicaNextIndex(it) - 1, log.size - 1)
                    val prevTerm = if (prevIndex >= 0) log[prevIndex].term  else 0
                    it.sendAppendEntriesReq(AppendEntriesReq(entry, currentTerm, prevIndex, prevTerm, commitIndex))
                    val maybeResponse = it.receiveAppendEntriesResp()
                    if (maybeResponse == null){
                        println("Leader: No AppendEntriesResp from $it. Should try again later")
                        break
                    }
                    val response = maybeResponse!!
                    val expected = AppendEntriesResp(currentTerm, true)
                    if (response != expected) {
                        println("Leader: No consensus for $entry")
                        if (!response.success && response.term > currentTerm) {
                            // [5.1]
                            return fallbackTo(State.FOLLOWER_INITIAL)
                        }
                        // [5.3]
                        nextIndex[it] = replicaNextIndex(it) - 1
                        if (replicaNextIndex(it) >= 0) {
                            entry = log[replicaNextIndex(it)]
                        }
                        if (sendHeartbeat(it, false) == null) {
                            return fallbackTo(State.CANDIDATE)
                        }
                        trackLog()
                    } else if (replicaNextIndex(it) == log.size - 1) {
                        followerIsDone = true
                    }
                    else {
                        nextIndex[it] = replicaNextIndex(it) + 1
                        if (replicaNextIndex(it) < log.size) {
                            entry = log[replicaNextIndex(it)]
                        } else {
                            entry = MetaEntry(Pair(id, value), currentTerm)
                        }
                        if (sendHeartbeat(it, false) == null) {
                            return fallbackTo(State.CANDIDATE)
                        }
                    }
                } while (!followerIsDone)
            }
            logState[id] = value
            // [5.3]
            followers.forEach {
                matchIndex[it] = replicaNextIndex(it)
                nextIndex[it] = replicaNextIndex(it) + 1
            }
            // [5.3] [5.4]
            val indexList = matchIndex.values.sorted()
            val majorityCommitIndex = if (log[indexList[indexList.size/2]].term == currentTerm) indexList[indexList.size/2] else 0
            commitIndex = max(majorityCommitIndex, commitIndex + 1)
            println("Leader: $id := $value")
        }
        followers.forEach {sendHeartbeat(it, true)}
        state = State.LEADER_DONE
        println("Leader: done with commitIndex=$commitIndex")
    }

    fun replicateEntries(entriesToReplicate_ : HashMap<Char, Int>) {
        entriesToReplicate = entriesToReplicate_
    }

    fun setupDelaysForReplicasChannel(delayedFollowers_ : Set<Follower>) {
        delayedFollowers = delayedFollowers_
    }

    private suspend fun sendHeartbeat(replica : Follower, done : Boolean) : Boolean? {
        if (replica in delayedFollowers) {
            delay(2*rpcTimeoutMs)
        }
        return replica.sendHeartbeat(done)
    }

    private fun fallbackTo(state : State) {
        this.state = state
        println("Leader: need to fallback to state=$state")
    }

    private fun replicaNextIndex(replica : Follower) : Int = nextIndex[replica]!!

    private val followers = followers
    private val nextIndex = HashMap<Follower, Int>()
    private val matchIndex = HashMap<Follower, Int>()
    private var entriesToReplicate = entriesToReplicate
    var delayedFollowers = setOf<Follower>()

    init {
        this.followers.forEach {
            nextIndex[it] = 0
            matchIndex[it] = -1
        }
    }
}

object Generator {
    fun nextInt() : Long = ++counter
    private var counter : Long = 0
}

class Candidate(expectedCandidates : Int, otherCandidates : List<Candidate>, logState : HashMap<Char, Int>, log : MutableList<MetaEntry>,
                state : State) : Node(logState, log, state) {

    override suspend fun run() {
        state = State.CANDIDATE
        println("Candidate $this: start")
        var endOfElection = false
        // only main happy path for now
        while (!endOfElection) {
            currentTerm++
            if (otherCandidates.size < expectedCandidates) {
                println("Candidate $this: too less candidates. Wait half of election timeout to catch up.")
                delay(rpcTimeoutMs/2)
                return
            }
            val maybeRequestVoteReq = receiveRequestVoteReq()
            if (maybeRequestVoteReq != null) {
                var vote  = true
                val requestVoteReq = maybeRequestVoteReq!!
                val maybeVoter = otherCandidates.find { it.hashCode() ==  requestVoteReq.candidateId }
                val voter = maybeVoter!!
                if (requestVoteReq.term >= currentTerm) {
                    if (requestVoteReq.lastLogIndex < 0) {
                    } else {
                        assert(false) // FIXME - test it
                        if (requestVoteReq.lastLogIndex < log.size) {
                            vote = log[requestVoteReq.lastLogIndex].term <= requestVoteReq.lastLogTerm
                        } else {
                            vote = false
                        }
                    }
                } else {
                   vote  = false
                }
                if (vote) {
                    println("Candidate $this: vote for $voter + transition to Follower")
                    sendRequestVoteResp(voter, RequestVoteResp(currentTerm, true))
                    endOfElection = true
                    state = State.FOLLOWER_INITIAL
                } else {
                    sendRequestVoteResp(voter, RequestVoteResp(currentTerm, false))
                }
            } else {
                var votesForMe = 0
                otherCandidates.forEach {
                    val lastLogIndex = log.size - 1
                    val lastLogTerm = if (lastLogIndex >= 0) log[lastLogIndex].term else 0
                    val requestVoteReq = RequestVoteReq(currentTerm, hashCode(), lastLogIndex, lastLogTerm)
                    sendRequestVoteReq(it, requestVoteReq)
                    val maybeRequestVoteResp = receiveRequestVoteResp(this)
                    if (maybeRequestVoteResp == null) { }
                    if (maybeRequestVoteResp!!.voteGranted) {
                        votesForMe++
                    }
                }
                if (votesForMe > otherCandidates.size/2) {
                    println("Candidate $this: become Leader")
                    state = State.LEADER_INITIAL
                    endOfElection = true
                }
            }
        }
    }

    fun setCandidates(otherCandidates : List<Candidate>) {
        this.otherCandidates = otherCandidates
    }

    private suspend fun sendRequestVoteReq(candidate : Candidate, requestVote : RequestVoteReq) {
        candidate.channel.send(requestVote)
    }

    private suspend fun receiveRequestVoteReq() : RequestVoteReq? {
        val timeoutMs = Generator.nextInt()*rpcTimeoutMs
        val message : Message? = withTimeoutOrNull(timeoutMs) { channel.receive() }
        return message as? RequestVoteReq
    }

    private suspend fun sendRequestVoteResp(candidate : Candidate, requestVoteResp : RequestVoteResp) {
        candidate.channel.send(requestVoteResp)
    }

    private suspend fun receiveRequestVoteResp(candidate : Candidate) : RequestVoteResp? {
        val timeoutMs = Generator.nextInt()*rpcTimeoutMs
        val message : Message? = withTimeoutOrNull(timeoutMs) { candidate.channel.receive() }
        return message as? RequestVoteResp
    }

    private var otherCandidates = otherCandidates
    private val channel = Channel<Message>()
    private val expectedCandidates = expectedCandidates
}

enum class Instance {
    FOLLOWER, LEADER, ARTIFICIAL_FOLLOWER
}

class Server(startingInstance : Instance, maybeEntriesToReplicate : HashMap<Char, Int>?, otherNodes : MutableList<Node>,
             stopOnStateChange : Boolean) : Node() {

    override suspend fun run() {
        while (true) {
            node.run()
            state = node.state
            if (stopOnStateChange) {
                return
            }
            when (state) {
                State.FOLLOWER_INITIAL -> {
                    node = Follower(logState, log, state)
                }
                State.LEADER_INITIAL -> {
                    val knownFollowers = otherNodes.map { it.me() }.filter { it is Follower } as List<Follower>
                    node = Leader(knownFollowers, maybeEntriesToReplicate!!, logState, log, state)
                }
                State.CANDIDATE -> {
                    val knownCandidates = otherNodes.map { it.me() }.filter { it is Candidate && it != me() } as List<Candidate>
                    if (node is Candidate) {
                        (node as Candidate).setCandidates(knownCandidates)
                    } else {
                        node = Candidate(otherNodes.size - 1, knownCandidates, logState, log, state)
                    }
                }
                State.FOLLOWER_DONE -> return
                State.LEADER_DONE -> return
            }
        }
    }

    override fun me() : Node = node

    private fun createNode(startingInstance : Instance) : Node {
        return when (startingInstance) {
            Instance.LEADER -> {
                val knownFollowers = otherNodes.map { it.me() }.filter { it is Follower } as List<Follower>
                Leader(knownFollowers, maybeEntriesToReplicate!!, logState, log, state)
            }
            Instance.FOLLOWER ->  Follower(logState, log, state)
            Instance.ARTIFICIAL_FOLLOWER ->  ArtificialFollower(logState, log, state)
        }
    }

    private val maybeEntriesToReplicate = maybeEntriesToReplicate
    private val otherNodes = otherNodes
    private var node : Node = createNode(startingInstance)
    private val stopOnStateChange = stopOnStateChange
}

class ArtificialFollower(logState : HashMap<Char, Int>, log : MutableList<MetaEntry>, state : State) : Follower(logState, log, state) {

    fun poison(term : Int, log : MutableList<MetaEntry>) {
        super.currentTerm = term
        super.log = log
        super.logState.clear()
        super.log.forEach { (entry, _) -> super.logState[entry.first] = entry.second }
    }
}

suspend fun launchServers(servers : List<Node>) = runBlocking {
    servers.forEach { launch {
        try {
            it.run()
        } catch (e : Exception) { println("$it: failed with:     $e") }
    } }
}

fun oneLeaderOneFollowerScenarioWithConsensus() = runBlocking {
    println("oneLeaderOneFollowerScenarioWithConsensus")
    val entriesToReplicate = hashMapOf('x' to 1, 'y' to 2)
    val nodes = mutableListOf<Node>()
    nodes.add(Server(Instance.FOLLOWER, null, nodes, false))
    nodes.add(Server(Instance.LEADER, entriesToReplicate, nodes, false))
    launchServers(nodes)
    nodes.forEach {
        val node = it.me()
        if (node is Follower) {
            assert(node.verifyLog(mutableListOf(MetaEntry('x' to 1, 1), MetaEntry('y' to 2, 1) )))
            assert(node.commitIndex == 2 && node.currentTerm == 1)
        }
    }
    println()
}

fun oneLeaderOneFollowerMoreEntriesScenarioWithConsensus() = runBlocking {
    println("oneLeaderOneFollowerMoreEntriesScenarioWithConsensus")
    val entriesToReplicate = hashMapOf('1' to 1, '2' to 2, '3' to 3, '4' to 2, '5' to 1, '6' to 3)
    val nodes = mutableListOf<Node>()
    nodes.add(Server(Instance.FOLLOWER, null, nodes, false))
    nodes.add(Server(Instance.LEADER, entriesToReplicate, nodes, false))
    launchServers(nodes)
    nodes.forEach {
        val node = it.me()
        if (node is Follower) {
            assert(node.verifyLog(mutableListOf(MetaEntry('1' to 1, 1),
                MetaEntry('2' to 2, 1), MetaEntry('3' to 3, 1), MetaEntry('4' to 2, 1),
                MetaEntry('5' to 1, 1), MetaEntry('6' to 3, 1))))
            assert(node.commitIndex == 6 && node.currentTerm == 1)
        }
    }
    println()
}

fun oneLeaderManyFollowersScenarioWithConsensus() = runBlocking {
    println("oneLeaderManyFollowersScenarioWithConsensus")
    val entriesToReplicate = hashMapOf('x' to 1, 'y' to 2)
    val nodes = mutableListOf<Node>()
    for (i in 0..12)
        nodes.add(Server(Instance.FOLLOWER, null, nodes, false))
    nodes.add(Server(Instance.LEADER, entriesToReplicate, nodes, false))
    launchServers(nodes)
    nodes.forEach {
        val node = it.me()
        if (node is Follower) {
            assert(node.verifyLog(mutableListOf(MetaEntry('x' to 1, 1), MetaEntry('y' to 2, 1) )))
            assert(node.commitIndex == 2 && node.currentTerm == 1)
        }
    }
    println()
}

fun oneLeaderManyFollowersWithArtificialOneScenarioWithConsensus() = runBlocking {
    println("oneLeaderManyFollowersWithArtificialOneScenarioWithConsensus")
    val entriesToReplicate = hashMapOf('x' to 1, 'y' to 2)
    val nodes = mutableListOf<Node>()
    for (i in 0..4)
        nodes.add(Server(Instance.FOLLOWER, entriesToReplicate, nodes, false))
    nodes.add(Server(Instance.ARTIFICIAL_FOLLOWER, null, nodes, false))
    nodes.add(Server(Instance.LEADER, entriesToReplicate, nodes, false))
    launchServers(nodes)
    val verify = { node : Follower ->
        assert(node.verifyLog(mutableListOf(MetaEntry('x' to 1, 1), MetaEntry('y' to 2, 1) )))
        assert(node.commitIndex == 2 && node.currentTerm == 1)
    }
    nodes.forEach {
        val node = it.me()
        if (node is Follower) {
            verify(node)
        } else  if (node is ArtificialFollower) {
            verify(node)
        }
    }
    println()
}

fun oneLeaderOneFollowerWithArtificialOneScenarioWithConsensus() = runBlocking {
    println("oneLeaderOneFollowerWithArtificialOneScenarioWithConsensus")
    val entries1 = hashMapOf('a' to 1, 'b' to 2)
    val nodes = mutableListOf<Node>()
    nodes.add(Server(Instance.ARTIFICIAL_FOLLOWER, null, nodes, false))
    val leader = Server(Instance.LEADER, entries1, nodes, false)
    nodes.add(leader)
    println("Term 1 - replicate entries1")
    launchServers(nodes)
    println("Term 2 - replicate entries2")
    val entries2 = hashMapOf('c' to 3, 'd' to 4)
    (leader.me() as Leader).replicateEntries(entries2)
    launchServers(nodes)
    val verify = { node : Follower ->
        assert(node.verifyLog(mutableListOf(MetaEntry('a' to 1, 1),
            MetaEntry('b' to 2, 1), MetaEntry('d' to 4, 2), MetaEntry('c' to 3, 2) )))
        assert(node.commitIndex == 4 && node.currentTerm == 2)
    }
    nodes.forEach {
        val node = it.me()
        if (node is Follower) {
            verify(node)
        } else  if (node is ArtificialFollower) {
            verify(node)
        }
    }
    println()
}

fun oneLeaderOneFollowerShouldCatchUpWithConsensus() = runBlocking {
    println("oneLeaderOneFollowerShouldCatchUpWithConsensus")
    val entries1 = hashMapOf('a' to 1, 'b' to 2)
    val entries2 = hashMapOf('c' to 3, 'd' to 4)
    val entries3 = hashMapOf('e' to 5)
    val nodes = mutableListOf<Node>()
    val follower = Server(Instance.ARTIFICIAL_FOLLOWER, null, nodes, false)
    nodes.add(follower)
    val leader = Server(Instance.LEADER, entries1, nodes, false)
    nodes.add(leader)
    val leaderInstance = leader.me() as Leader
    val followerInstance = follower.me() as ArtificialFollower
    println("Term 1 - replicate entries1")
    launchServers(nodes)

    leaderInstance.replicateEntries(entries2)
    println("Term 2 - replicate entries2")
    launchServers(nodes)

    followerInstance.poison(1, mutableListOf(MetaEntry('a' to 1, 1)))
    leaderInstance.replicateEntries(entries3)
    println("Term 3 - replicate entries3; follower log is going to be aligned")
    launchServers(nodes)

    val verify = { node : Follower ->
        assert(node.verifyLog(mutableListOf(MetaEntry('a' to 1, 1),
            MetaEntry('b' to 2, 1), MetaEntry('d' to 4, 2), MetaEntry('c' to 3, 2), MetaEntry('e' to 5, 3))))
        assert(node.commitIndex == 5 && node.currentTerm == 3)
    }
    nodes.forEach {
        val node = it.me()
        if (node is Follower) {
            verify(node)
        } else  if (node is ArtificialFollower) {
            verify(node)
        }
    }
    println()
}

fun oneLeaderOneFollowerShouldRemoveOldEntriesAndCatchUpWithConsensus() = runBlocking {
    println("oneLeaderOneFollowerShouldRemoveOldEntriesAndCatchUpWithConsensus")
    val entries1 = hashMapOf('a' to 1)
    val entries2 = hashMapOf('c' to 3, 'd' to 4)
    val entries3 = hashMapOf('e' to 5)
    val nodes = mutableListOf<Node>()
    val stopOnStateChange = true
    val follower = Server(Instance.ARTIFICIAL_FOLLOWER, null, nodes, stopOnStateChange)
    nodes.add(follower)
    val leader = Server(Instance.LEADER, entries1, nodes, stopOnStateChange)
    nodes.add(leader)
    val leaderInstance = leader.me() as Leader
    val followerInstance = follower.me() as ArtificialFollower
    println("Term 1 - replicate entries1")
    launchServers(nodes)

    leaderInstance.replicateEntries(hashMapOf())
    println("Term 2 - just bump Leader term")
    launchServers(nodes)

    leaderInstance.replicateEntries(entries2)
    println("Term 3 - replicate entries2")
    launchServers(nodes)

    followerInstance.poison(2, mutableListOf(MetaEntry('a' to 1, 1), MetaEntry('z' to 3, 2)))
    leaderInstance.replicateEntries(entries3)
    println("Term 4 - replicate entries3; follower log is going to be aligned")
    launchServers(nodes)

    val verify = { node : Follower ->
        assert(node.verifyLog(mutableListOf(MetaEntry('a' to 1, 1),
            MetaEntry('d' to 4, 3), MetaEntry('c' to 3, 3), MetaEntry('e' to 5, 4))))
        assert(node.commitIndex == 4 && node.currentTerm == 4)
    }
    nodes.forEach {
        val node = it.me()
        if (node is Follower) {
            verify(node)
        } else  if (node is ArtificialFollower) {
            verify(node)
        }
    }
    println()
}

fun oneLeaderOneFollowerShouldRemoveButNotAllOldEntriesAndCatchUpWithConsensus() = runBlocking {
    println("oneLeaderOneFollowerShouldRemoveButNotAllOldEntriesAndCatchUpWithConsensus")
    val entries1 = hashMapOf('a' to 1)
    val entries2 = hashMapOf('c' to 3, 'd' to 4)
    val entries3 = hashMapOf('e' to 5)
    val nodes = mutableListOf<Node>()
    val stopOnStateChange = true
    val follower = Server(Instance.ARTIFICIAL_FOLLOWER, null, nodes, stopOnStateChange)
    nodes.add(follower)
    val leader = Server(Instance.LEADER, entries1, nodes, stopOnStateChange)
    nodes.add(leader)
    val leaderInstance = leader.me() as Leader
    val followerInstance = follower.me() as ArtificialFollower
    println("Term 1 - replicate entries1")
    launchServers(nodes)

    leaderInstance.replicateEntries(hashMapOf())
    println("Term 2 & 3 - just bump Leader term")
    launchServers(nodes)
    leaderInstance.replicateEntries(hashMapOf())
    launchServers(nodes)

    leaderInstance.replicateEntries(entries2)
    println("Term 4 - replicate entries2")
    launchServers(nodes)

    followerInstance.poison(4, mutableListOf(MetaEntry('a' to 1, 1),
        MetaEntry('b' to 1, 1), MetaEntry('x' to 2, 2), MetaEntry('z' to 2, 2), MetaEntry('p' to 3, 3), MetaEntry('q' to 3, 3),
        MetaEntry('c' to 3, 4), MetaEntry('d' to 4, 4)))
    leaderInstance.replicateEntries(entries3)
    println("Term 5 - replicate entries3; follower log is going to be aligned")
    launchServers(nodes)

    val verify = { node : Follower ->
        assert(node.verifyLog(mutableListOf(MetaEntry('a' to 1, 1),
            MetaEntry('d' to 4, 4), MetaEntry('c' to 3, 4), MetaEntry('e' to 5, 5))))
        assert(node.commitIndex == 4 && node.currentTerm == 5)
    }
    nodes.forEach {
        val node = it.me()
        if (node is Follower) {
            verify(node)
        } else  if (node is ArtificialFollower) {
            verify(node)
        }
    }
    println()
}

fun oneFailingLeaderOneFollowerScenarioWithNoConsensus() = runBlocking {
    println("oneFailingLeaderOneFollowerScenarioWithNoConsensus")
    val entriesToReplicate = hashMapOf('x' to 1, 'y' to 2)
    val nodes = mutableListOf<Node>()
    val stopOnStateChange = true
    val follower = Server(Instance.FOLLOWER, entriesToReplicate, nodes, stopOnStateChange)
    nodes.add(follower)
    val leader = Server(Instance.LEADER, entriesToReplicate, nodes, stopOnStateChange)
    nodes.add(leader)
    val set = setOf(follower.me() as Follower)
    (leader.me() as Leader).setupDelaysForReplicasChannel(set)
    println("Term 1 - HeartBeat is delayed, all servers become candidates")
    launchServers(nodes)
    assert(leader.state == State.CANDIDATE && follower.state == State.CANDIDATE)
    println()
}

fun twoCandidatesInitiateElectionsOneWins() = runBlocking {
    println("twoCandidatesInitiateElectionsOneWins")
    val nodes = mutableListOf<Node>()
    val follower1 = Server(Instance.FOLLOWER, hashMapOf(), nodes, false)
    nodes.add(follower1)
    val follower2 = Server(Instance.FOLLOWER, hashMapOf(), nodes, false)
    nodes.add(follower2)
    println("All servers become candidates, one wins elections")
    launchServers(nodes)
    assert(follower1.state == State.LEADER_DONE || follower2.state == State.LEADER_DONE)
    println()
}

fun moreCandidatesInitiateElectionsOneWins() = runBlocking {
    println("moreCandidatesInitiateElectionsOneWins")
    val nodes = mutableListOf<Node>()
    for (i in 0..5) {
        val follower = Server(Instance.FOLLOWER, hashMapOf(), nodes, false)
        nodes.add(follower)
    }
    println("All servers become candidates, one wins elections")
    launchServers(nodes)
    nodes.forEach {
        val node = it.me()
        if (node is Follower) {
            assert(node.state == State.FOLLOWER_DONE)
        } else {
            assert(node.state == State.LEADER_DONE)
        }
    }
    println()
}

fun twoCandidatesInitiateElectionsOneWinsWithConsensus() = runBlocking {
    println("twoCandidatesInitiateElectionsOneWinsWithConsensus")
    val entriesToReplicate = hashMapOf('1' to 1, '2' to 2, '3' to 3, '4' to 4, '5' to 5, '6' to 6)
    val nodes = mutableListOf<Node>()
    val follower1 = Server(Instance.FOLLOWER, entriesToReplicate, nodes, false)
    nodes.add(follower1)
    val follower2 = Server(Instance.FOLLOWER, entriesToReplicate, nodes, false)
    nodes.add(follower2)
    println("All servers become candidates, one wins elections and replicate entries")
    launchServers(nodes)
    assert(follower1.state == State.LEADER_DONE || follower2.state == State.LEADER_DONE)
    nodes.forEach {
        val node = it.me()
        if (node is Follower) {
            assert(node.verifyLog(mutableListOf(MetaEntry('1' to 1, 1),
                MetaEntry('2' to 2, 1), MetaEntry('3' to 3, 1), MetaEntry('4' to 4, 1),
                MetaEntry('5' to 5, 1), MetaEntry('6' to 6, 1))))
            assert(node.commitIndex == 6 && node.currentTerm == 1)
            assert(node.state == State.FOLLOWER_DONE)
        }
    }
    println()
}

fun moreCandidatesInitiateElectionsOneWinsWithConsensus() = runBlocking {
    println("moreCandidatesInitiateElectionsOneWinsWithConsensus")
    val entriesToReplicate = hashMapOf('1' to 1, '2' to 2, '3' to 3, '4' to 4, '5' to 5, '6' to 6)
    val nodes = mutableListOf<Node>()
    for (i in 0..5) {
        val follower = Server(Instance.FOLLOWER, entriesToReplicate, nodes, false)
        nodes.add(follower)
    }
    println("All servers become candidates, one wins elections and replicate entries")
    launchServers(nodes)
    nodes.forEach {
        val node = it.me()
        if (node is Follower) {
            assert(node.verifyLog(mutableListOf(MetaEntry('1' to 1, 1),
                MetaEntry('2' to 2, 1), MetaEntry('3' to 3, 1), MetaEntry('4' to 4, 1),
                MetaEntry('5' to 5, 1), MetaEntry('6' to 6, 1))))
            assert(node.commitIndex == 6 && node.currentTerm == 1)
            assert(node.state == State.FOLLOWER_DONE)
        } else {
            assert(node.state == State.LEADER_DONE)
        }
    }
    println()
}

fun main() {
    oneLeaderOneFollowerScenarioWithConsensus()
    oneLeaderOneFollowerMoreEntriesScenarioWithConsensus()
    oneLeaderManyFollowersScenarioWithConsensus()
    oneLeaderManyFollowersWithArtificialOneScenarioWithConsensus()
    oneLeaderOneFollowerWithArtificialOneScenarioWithConsensus()
    oneLeaderOneFollowerShouldCatchUpWithConsensus()
    oneLeaderOneFollowerShouldRemoveOldEntriesAndCatchUpWithConsensus()
    oneLeaderOneFollowerShouldRemoveButNotAllOldEntriesAndCatchUpWithConsensus()
    oneFailingLeaderOneFollowerScenarioWithNoConsensus()
    twoCandidatesInitiateElectionsOneWins()
    moreCandidatesInitiateElectionsOneWins()
    twoCandidatesInitiateElectionsOneWinsWithConsensus()
    moreCandidatesInitiateElectionsOneWinsWithConsensus()
}
