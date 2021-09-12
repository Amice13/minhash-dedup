// Librarise for working with the filesystem
const fs = require('fs')
const { resolve } = require('path')

// Library for multiprocessing
const Pool = require('piscina')

// Process N folders simultaneously
const MAX_QUEUE = 8

// Start folder
const START_FOLDER = 'data'

/* DEFINE THE WORKER POOL */

// Create a worker pool to process folders
const pool = new Pool({
  filename: resolve(__dirname, 'worker.js'),
  maxQueue: MAX_QUEUE,
  idleTimeout: 1000 })

// If worker pools is drain then process another data
pool.on('drain', () => {
  if (foldersToProcess.length === 0) {
    waitToComplete()
  }
  while (pool.queueSize < MAX_QUEUE) {
    let datum = foldersToProcess.pop()
    if (datum) pool.run({ datum, iteration })
    return true
  }
})

/* HELPERS */

// Wait untill all workers completed their tasks
const waitToComplete = () => {
  return setTimeout(() => {
    if (pool.completed === toComplete) return start(`result-${iteration}`)
    return waitToComplete()
  }, 500)
}

// Get all subfolders
const scanFolder = mainDir => {
  // Get information about the folder content
  const folders = fs.readdirSync(mainDir)

  // If the folder is empty then skip
  if (folders.length === 0) return false

  // If the folder contains files then add to queue
  if (fs.lstatSync(`${mainDir}/${folders[0]}`).isFile()) {
    foldersToProcess.push(`${mainDir}`)
    return false
  }

  // Else scan the each subfolder
  for (let folder of folders) scanFolder(`${mainDir}/${folder}`)
}

let iteration = 0
let foldersToProcess = []
let toComplete = 0

let previousCount = 0

const start = (mainDir = START_FOLDER) => {
  iteration++
  foldersToProcess = []

  // Check the folder content
  scanFolder(mainDir)

  // If the folder is empty then finish
  if (foldersToProcess.length === 0) return true
  if (previousCount === foldersToProcess.length) return true
  previousCount = foldersToProcess.length

  toComplete = foldersToProcess.length + pool.completed
  // Create folder to keep deduplicated files
  if (!fs.existsSync(`result-${iteration}`)) fs.mkdirSync(`result-${iteration}`)

  // Start workers
  let datum = foldersToProcess.pop()
  pool.run({ datum, iteration })  
}

start()
