const fs = require('fs')
const { Minhash, LshIndex } = require('minhash')

const MAX_AMOUNT = 1000
const MAX_SIMILARITY = 0.5

module.exports = async ({ datum, iteration }) => {
  // Read and tokenize files
  let files = fs.readdirSync(datum)
  files = files.map(el => `${datum}/${el}`)
  let size = MAX_AMOUNT

  // If there are more files than maximum allowed amount
  if (files.length > MAX_AMOUNT) {
    // Find the optimal equal group size
    size = Math.ceil(files.length / (Math.ceil(files.length / MAX_AMOUNT) + 1))

    // Sort files by their sizes (no need to compare very different files)
    files = files.map(file => ({ file, size: fs.statSync(file).size }))
    files.sort((a, b) => a.size - b.size)
    files = files.map(el => el.file)
  }

  while (files.length) {
    let chunk = files.splice(0, size)
    // If there is only one file in folder then copy it to the next iteration
    if (chunk.size === 1) {
      fs.copyFileSync(chunk[0], makeFilePath(chunk[0], iteration))
      return true
    }
    // Read texts and make minhashes for them
    let hashes = []
    for (let f of chunk) hashes.push(readFile(f))

    let amount = hashes.length
    let keep = 0
    while (hashes.length > 0) {
      // Get the first hash to compare with others
      let currentHash = hashes[0]

      // If only one hash then copy file to the next iteration
      if (hashes.length === 1) {
        fs.copyFileSync(currentHash.filename, makeFilePath(currentHash.filename, iteration))
        keep++
        hashes = hashes.slice(1)
        continue
      }

      // Compare the first hash with others
      const comparisons = hashes.slice(1).map(el => compareHashes(currentHash, el))

      // Keep only big similarities
      const duplicates = comparisons.filter(el => el.similarity >= MAX_SIMILARITY)
      // If no duplicates then copy file to the next iteration
      if (duplicates.length === 0) {
        fs.copyFileSync(currentHash.filename, makeFilePath(currentHash.filename, iteration))
        keep++
        hashes = hashes.slice(1)
        continue
      }

      // Get the biggest file among duplicates
      duplicates.sort((a, b) => b.stringLength2 - a.stringLength2)
      let duplicatedFiles = duplicates.map(el => el.filename2)

      // If the first file is the smallest then skip it
      if (duplicates.stringLength1 < duplicates.stringLength2) {
        hashes = hashes.slice(1)
        continue
      }

      // Copy the first file to the next iteration and remove all duplicates
      fs.copyFileSync(currentHash.filename, makeFilePath(currentHash.filename, iteration))
      keep++
      hashes = hashes.slice(1)
      hashes = hashes.filter(el => !duplicatedFiles.includes(el.filename))
    }
  }
  return true
}

const makeFilePath = (folder, iteration) => {
  let path = folder.replace(/^[^/]*/, `result-${iteration}`)
  path = path.replace(/\/[^/]+\/(?=[^/]+$)/, '/')
  let newFolder = path.replace(/\/[^/]+$/, '')
  fs.mkdirSync(newFolder, { recursive: true })
  return path
}

function jaccard (hash1, hash2) {
  if (hash1.length !== hash2.length) {
    throw new Error('hashvalue counts differ')
  } else if (hash1.seed !== hash2.seed) {
    throw new Error('seed values differ')
  }
  let shared = 0
  for (let i = 0; i < hash1.hashvalues.length; i++) {
    shared += hash1.hashvalues[i] == hash2.hashvalues[i]
  }
  return shared / hash1.hashvalues.length
}

const readFile = (value) => {
  let buf = fs.readFileSync(value)
  let text = buf.toString()
  let cleanText = text.replace(/[.,:;/!?#$%*"№\(\)\[\]\/\\«»-]/g, ' ')
    .replace(/_{2,}/g, ' ')
    .replace(/\n/g, ' ')
    .replace(/\s+/g, ' ')
  let tokens = cleanText.split(/ /g)
  let hash = new Minhash()
  tokens.map(el => { hash.update(el) })
  return { hash, tokenLength: tokens.length, length: text.length, filename: value }
}

const compareHashes = (el1, el2) => {
  return {
    similarity: jaccard(el1.hash, el2.hash),
    tokenLength1: el1.tokenLength,
    tokenLength2: el2.tokenLength,
    tokenLengthDistance: Math.abs(el1.tokenLength - el2.tokenLength),
    stringLength1: el1.length,
    stringLength2: el2.length,
    stringLengthDistance: Math.abs(el1.length - el2.length),
    filename1: el1.filename,
    filename2: el2.filename
  }
}
