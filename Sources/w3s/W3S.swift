import Foundation
import ArgumentParser
import AsyncCommand

enum Errors: Error {
    case noToken
}

@main
struct W3S: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "w3s",
        abstract: "Pushes files to web3.storage",
        shouldDisplay: true,
        subcommands: [
            Token.self,
            PutCAR.self,
        ])
    
    
    func run() async throws {
        print(W3S.helpMessage())
    }
}


struct Token: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "token",
        abstract: "Sets your API token")
    
    @Argument(help: "Your web3.storage API token")
    var token: String
    
    func run() async throws {
        UserDefaults.standard.set(token, forKey: "token")
        print("Saved token: \(token)")
    }
}


struct PutCAR: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "put-car",
        abstract: "Uploads a car file")
    
    
    @Argument(help: "A CAR file to be split",
              completion: CompletionKind.file(extensions: ["car"]))
    var file: String
    
    
    @Option(name: .long, help: "The target size of the output chunks in MB, default is 50")
    var size: Int?
    
    @Option(name: .long, help: "The number of concurrent uploads allowed, default is 4")
    var concurrent: Int?
    
    @Option(name: .long, help: "Automatically clean up CAR chunks. Defaults to true.")
    var cleanup: Bool?
    
    @Option(name: .long, help: "Skips the chunking process. Useful for retries. Defaults to false.")
    var skipChunking: Bool?
    
    
    func run() async throws {
        guard let token = UserDefaults.standard.string(forKey: "token") else {
            print("No API token set")
            throw Errors.noToken
        }
        
        let skipChunk = skipChunking ?? false
        if !skipChunk {
            print("Splitting into \(size ?? 50)MB chunks")
            let splitCar = Command(name: "Split-CAR",
                                  command: "/opt/homebrew/bin/carbites",
                                  arguments: [
                                    "split", file,
                                    "--size", "\(size ?? 50)MB", "--strategy", "treewalk"
                                  ])
            
            try await splitCar.run()
        }
        
    
        let fileURL = URL(fileURLWithPath: file)
        let filename = fileURL.lastPathComponent
        let workingDir = fileURL.deletingLastPathComponent()
        
        
        let enumerator = FileManager.default.enumerator(at: workingDir, includingPropertiesForKeys: [.isRegularFileKey], options: [.skipsHiddenFiles, .skipsPackageDescendants]) { fileURL, err in
            print(err)
            return false
        }

        
        var files: [URL] = []

        if let enumerator = enumerator {
            for case let fileURL as URL in enumerator {
                do {
                    let fileAttributes = try fileURL.resourceValues(forKeys:[.isRegularFileKey])
                    if fileAttributes.isRegularFile! {
                        if let filename = fileURL.pathComponents.last {
                         
                            let regex = try NSRegularExpression(pattern: "-[\\d]*.car")
                            if regex.numberOfMatches(in: filename, range: NSMakeRange(0, filename.count)) > 0 {
                                files.append(fileURL)
                            }
                        }
                    }
                } catch { print(error) }
            }
        }


        let operationQueue = OperationQueue()
        operationQueue.maxConcurrentOperationCount = concurrent ?? 4
        
        let config = URLSessionConfiguration.default
        config.httpMaximumConnectionsPerHost = 1024
        config.waitsForConnectivity = true
        
        let urlSession = URLSession(configuration: config)
        
        print("Uploading..\n")
        var ops: [Operation] = []
        var complete = 0
        for fileURL in files {
            
            ops.append(BlockOperation {
                let semaphore = DispatchSemaphore(value: 0)
                
                let endpoint: URL = URL(string: "https://api.web3.storage/car")!
                var request = URLRequest(url: endpoint)
                request.httpMethod = "POST"
                request.addValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
                request.addValue("application/vnd.ipld.car", forHTTPHeaderField: "Content-Types")
                request.addValue("\(filename)", forHTTPHeaderField: "X-NAME")
                
                let task = urlSession.uploadTask(with: request, fromFile: fileURL) { data, response, error in
                    defer {
                        semaphore.signal()
                    }
                    
                    if let error = error {
                        print("Error uploading file: \(fileURL)")
                        print("Error: \(error.localizedDescription)")
                    }
                    
                    if let res = response as? HTTPURLResponse {
                        if res.statusCode == 429 {
                            print("Rate limit hit!")
                        }
                    }
                    
                    
                    complete += 1
                    print("\u{1B}[1A\u{1B}[K\(complete + 1) / \(ops.count + 1)")
                    
                    let clean = cleanup ?? true
                    if clean {
                        do {
                            try FileManager.default.removeItem(at: fileURL)
                        } catch {
                            print("Error cleaning up \(filename): \(error)")
                        }
                    }
                }
                
                task.resume()
                _ = semaphore.wait(wallTimeout: .distantFuture)
            })
            
            
        }
        
        operationQueue.addOperations(ops, waitUntilFinished: true)
        
        
        
        let getCID = Command(name: "get-cid",
                              command: "/opt/homebrew/bin/ipfs-car",
                              arguments: [
                                "--list-roots", file
                              ])
        try await getCID.run()
        
        let log = await getCID.log
        let cid = log.trimmingCharacters(in: .whitespacesAndNewlines)
        
        
        print("Upload complete: \(cid)")
    }
    
}
