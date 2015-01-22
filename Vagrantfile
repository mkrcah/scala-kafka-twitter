# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|

  config.vm.box = "precise64" # Ubuntu 12.04

  memory_mb = 768

  nodes = {
    'node-1' => "192.168.5.100",
    'node-2' => "192.168.5.101",
    'node-3' => "192.168.5.102"
  }

  nodes.each_with_index do |(short_name, ip), idx|

    config.vm.define short_name do |host|

      host.vm.network :private_network, ip: ip
      host.vm.hostname = short_name
      host.vm.provider :virtualbox do |vb|
        vb.customize ["modifyvm", :id, "--memory", memory_mb]
      end

      host.vm.provision :ansible do |ansible|
        ansible.playbook = "provisioning/ansible.yml"
        ansible.extra_vars = {
          nodes: nodes.values,
          node_seq: idx
        }
      end
    end

  end

end
