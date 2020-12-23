open System.Runtime.CompilerServices
open System.Threading
#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.Remote"
open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp


let args = Environment.GetCommandLineArgs()
let rnd = System.Random()

//"akka.tcp://RemoteFSharp@localhost:8777/user/server"
let addr = "akka.tcp://TwitterEngine@127.0.0.1:8777/user/RequestHandler"

let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                
            }
            remote {
                helios.tcp {
                    port = 8778
                    hostname = 127.0.0.1
                }
            }
        }")

let system = ActorSystem.Create("TwitterClient", configuration)
let twitterServer = system.ActorSelection(addr)

type Message =
    | Register of string
    | Login of string
    | Logout
    | Tweet of string
    | ReTweet
    | TweetsWithHashtag of string
    | Subscribe
    | GetMyTweets
    | Content of string * string
    | Start
    | StartWorking
    | Loop

let mutable tweetCount = 0


let mutable NumClients = 1000
let mutable maxSubscribers = 100
let mutable disconnectClient = 20


(*
let Client (username: int) (mailbox:Actor<_>) =
    let rec loop() = actor {
        let! msg = mailbox.Receive()
        //logic to randomly send tweet,retweet, register, login, logout ,etc to twitter server
        match msg with
        | Register(pass) ->
            //printfn "inside register"
            let msg = "Register|" + ("user" + (string username)) + "|"+(string pass) 
            twitterServer <! msg
        | Login(pass) ->
            twitterServer <! ("Login|"+("user" + (string username))+"|"+ (string pass))
        | Logout ->
            twitterServer <! ("Logout|" + ("user" + (string username)))
        | Tweet(s) -> //s is tweet content
            tweetCount <- tweetCount + 1
            twitterServer <! ("Tweet|" + ("user" + (string username)) + "|" + s)
            printfn "user%i posted tweet successfully" username 
        | ReTweet ->
            let rndTweetid = rnd.Next(0, tweetCount)
            twitterServer <! ("Retweet|" + ("user" + (string username)) + "|" + (rndTweetid |> string))
            printfn "User%i retweeted the %i tweet on his wall" username rndTweetid 
        | TweetsWithHashtag(s) ->
            twitterServer <! ("GetByHashTag|" + s)
            printfn "Fetching the top 5 tweets with hashtag"
        | Subscribe ->
            let rndClient = rnd.Next(1, NumClients+1)
            twitterServer <! ("Subscribe|"+("user" + (string username)) + "|user" + (string rndClient))
            printfn "User%i subscribed to user%i" username rndClient
        | GetMyTweets ->
            twitterServer <! ("GetTweets|" + ("user" + (string username)))
            printfn "Fetching all the user tweets for user %i" username
        | Content(ContentType, s) ->
            printfn "%s" s
        | _ -> printfn("Unknown command!!")
        return! loop()
    }
    loop()
    
    *)
    
let Client (username: int) (mailbox:Actor<_>) =
    let rec loop() = actor {
        let! msg = mailbox.Receive()
        //logic to randomly send tweet,retweet, register, login, logout ,etc to twitter server
        let command = (msg|>string).Split '|'
        if command.[0].CompareTo("Register") = 0 then
            //printfn "registration works"
            twitterServer <! ("Register|" + ("user" + (string username)) + "|"+(string command.[1])) 
        elif command.[0].CompareTo("Login") = 0 then
            //printfn "Login works"
            twitterServer <! ("Login|"+("user" + (string username))+"|"+ (string command.[1]))
        elif command.[0].CompareTo("Logout") = 0 then
            //printfn "Logout works"
            twitterServer <! ("Logout|" + ("user" + (string username)))
        elif command.[0].CompareTo("Tweet") = 0 then
            //printfn "Tweet works"
            tweetCount <- tweetCount + 1
            twitterServer <! ("Tweet|" + ("user" + (string username)) + "|" + command.[1])
        elif command.[0].CompareTo("Subscribe") = 0 then
            //printfn "subscribe works"
            let rndClient = rnd.Next(1, NumClients+1)
            twitterServer <! ("Subscribe|"+("user" + (string username)) + "|user" + (string rndClient))
        elif command.[0].CompareTo("ReTweet") = 0 then
            //printfn "retweet works"
            let rndTweetid = rnd.Next(0, tweetCount)
            twitterServer <! ("Retweet|" + ("user" + (string username)) + "|" + (rndTweetid |> string))
        elif command.[0].CompareTo("GetTweets") = 0 then
            //printfn "getweets works"
            twitterServer <! ("GetTweets|" + ("user" + (string username)))
        elif command.[0].CompareTo("GetByHashTag") = 0 then
            //printfn "getbyhashtag works"
            twitterServer <! ("GetByHashTag|" + command.[1])
        else
            printfn "Response messages%s" msg
           
        return! loop()
        
        return! loop()
    }
    loop()

if args.Length>3 then
    NumClients <- int args.[3] //Num clients

let Simulator (mailbox:Actor<_>)=
    let clients = [for a in 0 .. NumClients do yield(spawn system ("Client_" + (string a)) (Client a))]
    let rec loop() = actor {
        let! msg = mailbox.Receive()
        match msg with
        | Start ->
            for i in 1..NumClients do
                clients.[i] <! ("Register|password"+(string i))
            printfn "Computing zipf distribution..."
            mailbox.Self <! StartWorking
        | StartWorking ->
            printfn "Server started and processing.."
            for i in 1..NumClients do
                clients.[i] <! ("Login|password"+(string i))
            mailbox.Self <! Loop
        | Loop ->
            let randomNumber = rnd.Next(0, 1000) % 15
            match randomNumber with
            | 0 | 12 | 13 | 14 -> // tweet
                let mutable rndClient = rnd.Next(1, NumClients+1)
                let rndMention = rnd.Next(1, NumClients+1)
                if rnd.Next(0,2) = 1 then
                    mailbox.Context.System.Scheduler.ScheduleTellOnce (TimeSpan.FromMilliseconds (20. * (float rndClient)), clients.[rndClient], "Tweet|tweeting @user" + (string rndMention))
                //    if rnd.Next(0,3) = 2 then
                //        printfn "%s" ("tweeting @" + (string rndMention))
                else
                    mailbox.Context.System.Scheduler.ScheduleTellOnce (TimeSpan.FromMilliseconds (20. * (float rndClient)), clients.[rndClient], "Tweet|tweeting that #COP5615isgreat")
                //    if rnd.Next(0,3) = 2 then
                //        printfn "%s" ("tweeting that #COP5615isgreat")  
                mailbox.Self <! Loop
            | 1 | 2 | 3 -> //retweet
                let rndClient = rnd.Next(1, NumClients+1)
                mailbox.Context.System.Scheduler.ScheduleTellOnce (TimeSpan.FromMilliseconds (20. * (float rndClient)), clients.[rndClient], "ReTweet")
                mailbox.Self <! Loop
            | 4 -> // getTweetsWithHashTag
                let rndClient = rnd.Next(1, NumClients+1)
                mailbox.Context.System.Scheduler.ScheduleTellOnce (TimeSpan.FromMilliseconds (20. * (float rndClient)), clients.[rndClient], "GetByHashTag|#COP5615isgreat")
                mailbox.Self <! Loop
            | 5 | 6 -> // Subscribe to other user tweets
                let mutable rndClient = rnd.Next(1, NumClients+1)
                mailbox.Context.System.Scheduler.ScheduleTellOnce (TimeSpan.FromMilliseconds (20. * (float rndClient)), clients.[rndClient], "Subscribe")
                mailbox.Self <! Loop
            | 7 -> // Get My Tweets
                //printfn "getmy tweets"
                let rndClient = rnd.Next(1, NumClients+1)
                mailbox.Context.System.Scheduler.ScheduleTellOnce (TimeSpan.FromMilliseconds (20. * (float rndClient)), clients.[rndClient], "GetTweets")
                mailbox.Self <! Loop
            | 8 | 9 | 10 -> // Login
                let rndClient = rnd.Next(1, NumClients+1)
                mailbox.Context.System.Scheduler.ScheduleTellOnce (TimeSpan.FromMilliseconds (20. * (float rndClient)), clients.[rndClient], "Login|password"+(string rndClient))
                mailbox.Self <! Loop
            | 11 -> // Logout
                //printfn " logout"
                let rndClient = rnd.Next(1, NumClients+1)
                mailbox.Context.System.Scheduler.ScheduleTellOnce (TimeSpan.FromMilliseconds (20. * (float rndClient)), clients.[rndClient], "Logout")
                mailbox.Context.System.Scheduler.ScheduleTellOnce (TimeSpan.FromMilliseconds (18. * (float rndClient)), clients.[rndClient], "Login|password"+(string rndClient))
                mailbox.Self <! Loop
            
        return! loop()
    }
    loop()
    
let simRef = spawn system "TwitterClient" Simulator

simRef <! Start

//let clientRef = spawn system "client" (Client 1)

//clientRef <! "Register|user1|pass1"

system.WhenTerminated.Wait()