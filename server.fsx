open System.Threading
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.Remote"
open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Data
open System.Collections.Generic
open System.Text.RegularExpressions

let rnd = System.Random()
let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                debug : {
                    receive : on
                    autoreceive : on
                    lifecycle : on
                    event-stream : on
                    unhandled : on
                }
            }
            remote {
                helios.tcp {
                    port = 8777
                    hostname = 127.0.0.1
                }
            }
        }")

let system = ActorSystem.Create("TwitterEngine", configuration)

let mutable stopWatch = System.Diagnostics.Stopwatch.StartNew()
stopWatch.Stop()

type ActorMsg =
    | WorkerMsg of int64*int64*int64*int64
    | DispatcherMsg of int64*int64*int64
    | EndMsg of int64*int64
    | ResultMsg of int64
    | RegisterUserMsg of string * string //userid and password
    | TweetMsg of string * int  // tweet and userid
    | TweetSubsTo of int // user id
    | TweetWithHashtag of string * int // hashtag and userid
    | TweetWithMention of int  // user id
    | GetMyTweets of int // user id
    | AddSubscriber of int * int // user id and user id
    | DisconnectUser of int // user id
    | LoginUser of string * string // user id and password
    | Tweet of string * string * IActorRef
    | Register of string * string * IActorRef
    | Login of string * string* IActorRef
    | Logout of string* IActorRef
    | Content of string * string* IActorRef
    | ReTweet of string * string* IActorRef
    | Subscribe of string * string* IActorRef
    | GetTweets of string* IActorRef
    | GetByHashTag of string* IActorRef


let mutable clientRegistry=Map.empty<string,string> // key userid and value is password
let mutable tweets=Map.empty<string,list<string>> // key userid and value is tweet
let mutable hashtagToTweets = Map.empty<string,list<string>>
let mutable hashtagToUsers = Map.empty<string,list<string>>  // key userid and value hashtag
let mutable subscribedTo=Map.empty<string,list<string>> // key userid and value is list of user id's
let mutable followers=Map.empty<string,list<string>> // key userid and value is list of user id's
let mutable LoggedinUsers = Map.empty<string, int>
let mutable userToTweets = Map.empty<string, list<string>> //user to list of tweet ids


let mutable tweetList = [||]

let RegistrationHandler (mailbox:Actor<_>)=

    let RegisterUser(userid,pswd):unit=
        clientRegistry<-clientRegistry.Add(userid,pswd)

    let LoginUser(username, pass) =
        let mutable s = "Username and password don't match"
        if clientRegistry.ContainsKey(username) then
            if LoggedinUsers.ContainsKey((string username)) then
                s <- "User reconnected"
            elif clientRegistry.Item(username) = pass then
                LoggedinUsers.Add((string username), 1)
                s <- "User logged in successfully"
        s
    
    let LogoutUser(username) =
        let mutable s = "User already logged out"
        if LoggedinUsers.ContainsKey(username) then
            LoggedinUsers.Remove(username)
            s <- "User logged out successfully"
        s

    let rec loop() = actor{
        let! msg = mailbox.Receive()
        // printfn "%A" msg
        match msg with
        | Register(username,pass, act) ->
            RegisterUser((string username), (string pass))
            //act <! "Registered user%i Succesfully" username
        | Login (username, pass, act) ->
            LoginUser(username, pass)
            //act <! "Loggedin user%i Succesfully" username
        | Logout (username, act) ->
            LogoutUser(username)
            //act <! "Loggedout user%i Succesfully" username
        return! loop()
    }
    loop()


let TweetHandler (mailbox:Actor<_>) =
    let timer = System.Diagnostics.Stopwatch()

    let getMyTweets(userid)=
        let mutable lst = []
        if userToTweets.ContainsKey(userid) then
            lst <- userToTweets.Item(userid)
        lst

    let subscribeTo(userA, userB) =
        let mutable arr = []
        if followers.ContainsKey(userB) then
            arr <-  followers.Item(userB)
        arr <- arr @ [userA]
        followers <- followers.Add(userB, arr)      

    let getTweetswithHashTag(tag) =
        let mutable lst = [] 
        //printfn "hastagtotweets: %A" hashtagToTweets
        if hashtagToTweets.ContainsKey(tag) then
            lst <- hashtagToTweets.Item(tag)
        lst

    let reTweet(username, tweetID) =
        let mutable arr = []
        if userToTweets.ContainsKey(username) then
            arr <- userToTweets.Item(username)
        arr <- arr @ [(string tweetID)]
        userToTweets <- userToTweets.Add(username, arr)
        if followers.ContainsKey(username) then
            for f in followers.Item(username) do
                let mutable arr = []
                if userToTweets.ContainsKey(f) then
                    arr <- userToTweets.Item(f)
                arr <- arr @ [string tweetID]
                userToTweets.Add(f, arr)
    
    let ProcessTweets(username, tweetstring):unit=
        let tweetID = "tweet_" + (string tweetList.Length)
        tweetList <- Array.append tweetList [|tweetstring|]
        let mutable arr = []
        if tweets.ContainsKey(tweetID) then
            arr <- tweets.Item(tweetID)
        
        arr <- arr @ [username]
        //printfn "Tweets: %A" arr
        //printfn "Adding %A to tweets" arr
        tweets <- tweets.Add(tweetID, arr)
        let mutable arr1 = []
        if userToTweets.ContainsKey(username) then
            arr1 <- userToTweets.Item(username)
        arr1<- arr1 @ [tweetID]
        //printfn "Adding %A to userToTweets" arr1 
        userToTweets <- userToTweets.Add(username, arr1)
        let hashTagRegex = new Regex("#+[a-zA-Z0-9(_)]{1,}")
        
        let  hlist = hashTagRegex.Matches tweetstring    // enum concat not yet implemented
        //printfn "hlist count: %i" hlist.Count
        for i in 0..hlist.Count-1 do
            //printfn "%s" (hlist.Item(i).Value)
            if hashtagToTweets.ContainsKey (hlist.Item(i).Value) then
                let mutable arr = hashtagToTweets.Item(hlist.Item(i).Value)
                arr <- arr @ [(string tweetID)]
                hashtagToTweets <- hashtagToTweets.Add(hlist.Item(i).Value, arr)        
            else
                let mutable arr = []
                arr <- arr @ [(string tweetID)]
                hashtagToTweets <- hashtagToTweets.Add(hlist.Item(i).Value, arr)
        
        let mentionRegex = new Regex("@+[a-zA-Z0-9(_)]{1,}")
        let mutable mlist = mentionRegex.Matches tweetstring
        //printfn "Mlist count: %i" mlist.Count
        for i in 0..mlist.Count-1 do
            //printfn "%s" (mlist.Item(i).Value)
            if hashtagToTweets.ContainsKey (mlist.Item(i).Value) then
                let mutable arr = hashtagToTweets.Item(mlist.Item(i).Value)
                arr <- arr @ [(string tweetID)]
                hashtagToTweets <- hashtagToTweets.Add(mlist.Item(i).Value, arr)        
            else
                let mutable arr = []
                arr <- arr @ [(string tweetID)]
                hashtagToTweets <- hashtagToTweets.Add(mlist.Item(i).Value, arr)
            
        if followers.ContainsKey(username) then
            let lst = followers.Item(username)
            for f in lst do
                let mutable g = []
                if userToTweets.ContainsKey(f) then
                    g <- userToTweets.Item(f)
                g <- g@[tweetID]
                userToTweets <- userToTweets.Add(f, g)
    let rec loop() = actor {
        let! msg = mailbox.Receive()
        match msg with
        | Tweet(username, s, act) ->
            stopWatch <- System.Diagnostics.Stopwatch.StartNew()
            ProcessTweets(username, s)
            act <! ("User" + username + "tweeted successfully") 
            stopWatch.Stop()
            //printfn "The time to Tweet  %f" stopWatch.Elapsed.TotalMilliseconds
        | ReTweet(username, tweetID, act) ->
            stopWatch <- System.Diagnostics.Stopwatch.StartNew()
            reTweet(username, "tweet_"+tweetID)
            act <! ("User" + username + "retweeted successfully")
            stopWatch.Stop()
            //printfn "The time to retweet is %f" stopWatch.Elapsed.TotalMilliseconds
        | Subscribe(userA, userB, act) ->
            stopWatch <- System.Diagnostics.Stopwatch.StartNew()
            subscribeTo(userA, userB)
            act <! "user subscribed succesfully" 
            stopWatch.Stop()
            //printfn "The time to subscribe is %f" stopWatch.Elapsed.TotalMilliseconds
        | GetTweets(username, act) ->
            stopWatch <- System.Diagnostics.Stopwatch.StartNew()
            let lst = getMyTweets(username)
            //printfn "mytweets: %A" lst
            let last n xs = Seq.skip ((Seq.length xs) - n) xs
            let top5 = last 5 lst
            
            let mutable s = ""
            for i in top5 do
                s <- s + (string i)
            if s.Length <> 0 then
                act <! "Sending Top 5 tweets to users" + s
            stopWatch.Stop()
            //printfn "The time to getTweeets is %f" stopWatch.Elapsed.TotalMilliseconds
        | GetByHashTag(tag, act) ->
            stopWatch <- System.Diagnostics.Stopwatch.StartNew()
            let lst = getTweetswithHashTag(tag)
            let last n xs = Seq.skip ((Seq.length xs) - n) xs
            let top5 = last 5 lst
            let mutable s = ""
            for i in top5 do
                s <- s + (string i)
            if s.Length <> 0 then
                act <! "Sending tweets by hashtag" + s
            stopWatch.Stop()
            //printfn "The time to getHashTag is %f" stopWatch.Elapsed.TotalMilliseconds
        | _ -> printfn ("Unknown message")

        return! loop()
    }
    loop()

let TweetHandlerRef = spawn system "TweetHandler" TweetHandler
let registrationHandlerRef = spawn system "registractionHandler" RegistrationHandler

let mutable registrations = 0
let mutable nLogins = 0
let mutable nsubs = 0
let mutable nretweets = 0
let mutable nhashtags = 0

let mutable registrationsSW = System.Diagnostics.Stopwatch.StartNew()

let mutable LoginSW = System.Diagnostics.Stopwatch.StartNew()

let mutable ntweets = 0

let mutable tweetSW = System.Diagnostics.Stopwatch.StartNew()
let mutable subSW = System.Diagnostics.Stopwatch.StartNew()
let mutable retweetSW = System.Diagnostics.Stopwatch.StartNew()
let mutable hashtagSW = System.Diagnostics.Stopwatch.StartNew()


let commlink = 
    spawn system "RequestHandler"
    <| fun mailbox ->
        let mutable reqid = 0
        let rec loop() =
            actor {
                let! msg = mailbox.Receive()
                //printfn "msg: %s" msg
                reqid <- reqid + 1
                let command = (msg|>string).Split '|'
                if command.[0].CompareTo("Register") = 0 then
                    registrations <- registrations + 1
                    registrationHandlerRef <! Register(command.[1], command.[2], mailbox.Sender())
                    if registrations % 1000 = 0 then
                        registrationsSW.Stop()
                        printfn "The time of register users %i is %f" 1000 registrationsSW.Elapsed.TotalMilliseconds 
                    //mailbox.Sender() <! "This is server message"
                elif command.[0].CompareTo("Login") = 0 then
                    nLogins <- nLogins + 1
                    registrationHandlerRef <! Login(command.[1],command.[2], mailbox.Sender())
                    if nLogins % 1000 = 1 then
                        LoginSW.Stop()
                        printfn "The time of Login users %i is %f" 1000 LoginSW.Elapsed.TotalMilliseconds
                    //mailbox.Sender() <! "This is server message"
                elif command.[0].CompareTo("Logout") = 0 then
                    registrationHandlerRef <! Logout(command.[1], mailbox.Sender())
                    //mailbox.Sender() <! "This is server message"
                elif command.[0].CompareTo("Tweet") = 0 then
                    TweetHandlerRef <! Tweet(command.[1], command.[2], mailbox.Sender())
                    ntweets <- ntweets + 1
                    if ntweets % 1000 = 0 then
                        tweetSW.Stop()
                        printfn "The time of tweets users %i is %f" 1000 tweetSW.Elapsed.TotalMilliseconds
                    
                    //mailbox.Sender() <! "This is server message"
                elif command.[0].CompareTo("Subscribe") = 0 then
                    nsubs <- nsubs + 1
                    TweetHandlerRef <! Subscribe(command.[1], command.[2], mailbox.Sender())
                    if nsubs % 1000 = 0 then
                        subSW.Stop()
                        printfn "The time of subscribe users %i is %f" 1000 subSW.Elapsed.TotalMilliseconds
                    //mailbox.Sender() <! "This is server message"
                elif command.[0].CompareTo("Retweet") = 0 then
                    nretweets <- nretweets + 1
                    TweetHandlerRef <! ReTweet(command.[1], command.[2], mailbox.Sender())
                    if nretweets % 1000 = 0 then
                        retweetSW.Stop()
                        printfn "The time of retweet users %i is %f" 1000 retweetSW.Elapsed.TotalMilliseconds
                    //mailbox.Sender() <! "This is server message"
                elif command.[0].CompareTo("GetTweets") = 0 then
                    TweetHandlerRef <! GetTweets(command.[1], mailbox.Sender())
                    //mailbox.Sender() <! "This is server message"
                elif command.[0].CompareTo("GetByHashTag") = 0 then
                    nhashtags <- nhashtags + 1
                    TweetHandlerRef <! GetByHashTag(command.[1], mailbox.Sender())
                    if nhashtags % 1000 = 0 then
                        retweetSW.Stop()
                        printfn "The time of hashtag users %i is %f" 1000 hashtagSW.Elapsed.TotalMilliseconds
                    //mailbox.Sender() <! "This is server message"
                return! loop() 
            }
        loop()
        
printfn "Server Started"

system.WhenTerminated.Wait()