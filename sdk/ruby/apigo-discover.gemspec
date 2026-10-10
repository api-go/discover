Gem::Specification.new do |s|
  s.name = 'apigo-discover'
  s.version = '1.5.16'
  s.summary = 'Process-global Discover SDK'
  s.authors = ['Apigo']
  s.files = Dir['lib/**/*.rb', 'README.md']
  s.required_ruby_version = '>= 2.6'
  s.add_dependency 'redis', '>= 4.8', '< 6'
  s.add_dependency 'websocket-client-simple', '~> 0.9'
end
