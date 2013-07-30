//
//  EDQueue.m
//  queue
//
//  Created by Andrew Sliwinski on 6/29/12.
//  Copyright (c) 2012 Andrew Sliwinski. All rights reserved.
//

#import "EDQueue.h"
#import "EDQueueStorageEngine.h"

//

#define DEFINE_SHARED_INSTANCE_USING_BLOCK(block) \
static dispatch_once_t pred = 0; \
__strong static id _sharedObject = nil; \
dispatch_once(&pred, ^{ \
_sharedObject = block(); \
}); \
return _sharedObject; \

NSString *const EDQueueDidStart = @"EDQueueDidStart";
NSString *const EDQueueDidStop = @"EDQueueDidStop";
NSString *const EDQueueJobDidSucceed = @"EDQueueJobDidSucceed";
NSString *const EDQueueJobDidFail = @"EDQueueJobDidFail";
NSString *const EDQueueDidDrain = @"EDQueueDidDrain";

@interface EDQueue ()
@property EDQueueStorageEngine *engine;
@property (readwrite) Boolean isRunning;
@property (readwrite) Boolean isActive;
@property (readwrite) NSString *activeTask;
@property UIBackgroundTaskIdentifier backgroundTask;
@end

//

@implementation EDQueue

#pragma mark - Init

+ (EDQueue *)sharedInstance
{
    DEFINE_SHARED_INSTANCE_USING_BLOCK(^{
        return [[self alloc] init];
    });
}

- (id)init
{
    self = [super init];
    if (self) {
        _engine     = [[EDQueueStorageEngine alloc] init];
        _isRunning  = false;
        _isActive   = false;
        _retryLimit = 4;
        _backgroundTask = UIBackgroundTaskInvalid;
    }
    return self;
}

#pragma mark - Public methods

/**
 * Adds a new job to the queue.
 *
 * @param {id} Data
 * @param {NSString} Task label
 *
 * @return {void}
 */
- (void)enqueueWithData:(id)data forTask:(NSString *)task
{
    if (data == nil) data = @{};
    [self.engine createJob:data forTask:task];
    [self tick];
}

/**
 * Returns true if a job exists for this task.
 *
 * @param {NSString} Task label
 *
 * @return {Boolean}
 */
- (Boolean)jobExistsForTask:(NSString *)task
{
    Boolean jobExists = [self.engine jobExistsForTask:task];
    return jobExists;
}

/**
 * Returns true if the active job if for this task.
 *
 * @param {NSString} Task label
 *
 * @return {Boolean}
 */
- (Boolean)jobIsActiveForTask:(NSString *)task
{
    Boolean jobIsActive = [self.activeTask length] > 0 && [self.activeTask isEqualToString:task];
    return jobIsActive;
}

/**
 * Returns the list of jobs for this 
 *
 * @param {NSString} Task label
 *
 * @return {NSArray}
 */
- (NSDictionary *)nextJobForTask:(NSString *)task
{
    NSDictionary *nextJobForTask = [self.engine fetchJobForTask:task];
    return nextJobForTask;
}

/**
 * Starts the queue.
 *
 * @return {void}
 */
- (void)start
{
    if (!self.isRunning) {
        self.isRunning = true;
        [self tick];
        [self performSelectorOnMainThread:@selector(postNotification:) withObject:[NSDictionary dictionaryWithObjectsAndKeys:EDQueueDidStart, @"name", nil, @"data", nil] waitUntilDone:false];
    }
}

/**
 * Stops the queue.
 * @note Jobs that have already started will continue to process even after stop has been called.
 *
 * @return {void}
 */
- (void)stop
{
    if (self.isRunning) {
        self.isRunning = false;
        [self performSelectorOnMainThread:@selector(postNotification:) withObject:[NSDictionary dictionaryWithObjectsAndKeys:EDQueueDidStop, @"name", nil, @"data", nil] waitUntilDone:false];
    }
}



/**
 * Empties the queue.
 * @note Jobs that have already started will continue to process even after empty has been called.
 *
 * @return {void}
 */
- (void)empty
{
    [self.engine removeAllJobs];
}


#pragma mark - Private methods

- (void)beginBackgroundTask
{
    if (self.backgroundTask == UIBackgroundTaskInvalid && [[UIDevice currentDevice] isMultitaskingSupported]) {
        self.backgroundTask = [[UIApplication sharedApplication] beginBackgroundTaskWithExpirationHandler:^{
            [self endBackgroundTask];
        }];
    }
}

- (void)endBackgroundTask
{
    if (self.backgroundTask != UIBackgroundTaskInvalid && [[UIDevice currentDevice] isMultitaskingSupported]) {
        [[UIApplication sharedApplication] endBackgroundTask:self.backgroundTask];
        self.backgroundTask = UIBackgroundTaskInvalid;
    }
}

/**
 * Checks the queue for available jobs, sends them to the processor delegate, and then handles the response.
 *
 * @return {void}
 */
- (void)tick
{
    dispatch_queue_t gcd = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_BACKGROUND, 0);
    dispatch_async(gcd, ^{
        if (self.isRunning && !self.isActive && [self.engine fetchJobCount] > 0) {
            // Begin background task if not already done
            [self beginBackgroundTask];
            
            // Start job
            self.isActive = true;
            id job = [self.engine fetchJob];
            self.activeTask = [(NSDictionary *)job objectForKey:@"task"];
            
            // Pass job to delegate
            if ([self.delegate respondsToSelector:@selector(queue:processJob:completion:)]) {
                [self.delegate queue:self processJob:job completion:^(EDQueueResult result) {
                    [self processJob:job withResult:result];
                    self.activeTask = nil;
                }];
            } else {
                EDQueueResult result = [self.delegate queue:self processJob:job];
                [self processJob:job withResult:result];
                self.activeTask = nil;
            }
        }
        
        // End background task if queue is no longer running
        if (!self.isRunning) {
            [self endBackgroundTask];
        }
    });
}

- (void)processJob:(NSDictionary*)job withResult:(EDQueueResult)result
{
    // Check result
    switch (result) {
        case EDQueueResultSuccess:
            [self performSelectorOnMainThread:@selector(postNotification:) withObject:[NSDictionary dictionaryWithObjectsAndKeys:EDQueueJobDidSucceed, @"name", job, @"data", nil] waitUntilDone:false];
            [self.engine removeJob:[job objectForKey:@"id"]];
            break;
        case EDQueueResultFail: {
            NSUInteger currentAttempt = [[job objectForKey:@"attempts"] intValue] + 1;
            BOOL willRetry = currentAttempt < self.retryLimit;
            [self performSelectorOnMainThread:@selector(postNotification:) withObject:[NSDictionary dictionaryWithObjectsAndKeys:EDQueueJobDidFail, @"name", job, @"data", [NSNumber numberWithBool:willRetry], @"willRetry", nil] waitUntilDone:true];
            if (willRetry) {
                [self.engine incrementAttemptForJob:[job objectForKey:@"id"]];
            } else {
                [self.engine removeJob:[job objectForKey:@"id"]];
            }
            break;
        }
        case EDQueueResultCritical:
            [self performSelectorOnMainThread:@selector(postNotification:) withObject:[NSDictionary dictionaryWithObjectsAndKeys:EDQueueJobDidFail, @"name", job, @"data", [NSNumber numberWithBool:NO], @"willRetry", nil] waitUntilDone:false];
            [self errorWithMessage:@"Critical error. Job canceled."];
            [self.engine removeJob:[job objectForKey:@"id"]];
            break;
    }
    
    // Clean-up
    self.isActive = false;
    
    // Drain
    if ([self.engine fetchJobCount] == 0) {
        [self performSelectorOnMainThread:@selector(postNotification:) withObject:[NSDictionary dictionaryWithObjectsAndKeys:EDQueueDidDrain, @"name", nil, @"data", nil] waitUntilDone:false];
        // End background task if queue is drained
        [self endBackgroundTask];
    } else {
        [self performSelectorOnMainThread:@selector(tick) withObject:nil waitUntilDone:false];
    }
}

/**
 * Posts a notification (used to keep notifications on the main thread).
 *
 * @param {NSDictionary} Object
 *                          - name: Notification name
 *                          - data: Data to be attached to notification
 *
 * @return {void}
 */
- (void)postNotification:(NSDictionary *)object
{
    [[NSNotificationCenter defaultCenter] postNotificationName:[object objectForKey:@"name"] object:[object objectForKey:@"data"] userInfo:[NSDictionary dictionaryWithObjectsAndKeys:[object objectForKey:@"willRetry"], @"willRetry", nil]];
}

/**
 * Writes an error message to the log.
 *
 * @param {NSString} Message
 *
 * @return {void}
 */
- (void)errorWithMessage:(NSString *)message
{
    NSLog(@"EDQueue Error: %@", message);
}

#pragma mark - Dealloc

- (void)dealloc
{    
    self.delegate = nil;
    _engine = nil;
}

@end
